"""
report_core.py — shared data, helpers, translations, and section builder
for the Maldini Stats dashboard.

Both terminal_app.py (Streamlit) and cli_app.py (CLI) import from here.
"""

import re
from datetime import datetime

from google.cloud import bigquery

# ── constants ─────────────────────────────────────────────────────────────────
PROJECT = "maldinia"
MARTS   = f"{PROJECT}.dbt_dev_marts"

BENCHMARKS = {
    "naive_baseline": 0.2222,
    "bookmaker":      0.19,
    "threshold":      0.20,
}

W  = 72
HW = 13
AW = 13


# ── data ──────────────────────────────────────────────────────────────────────
def load(credentials=None):
    bq = bigquery.Client(project=PROJECT, credentials=credentials)

    summary = dict(
        bq.query(f"SELECT * FROM `{MARTS}.mart_scores_summary`").result().__next__()
    )
    std_row = dict(
        bq.query(
            f"SELECT STDDEV(brier_score) as std_brier FROM `{MARTS}.fct_predictions`"
        ).result().__next__()
    )
    comp = bq.query(
        f"SELECT competition, avg_brier_score as avg_brier, prediction_count as n "
        f"FROM `{MARTS}.mart_competition_summary` ORDER BY avg_brier_score"
    ).to_dataframe()

    monthly_df = bq.query(
        f"SELECT * FROM `{MARTS}.mart_monthly_scores` ORDER BY month"
    ).to_dataframe()
    monthly_df["quarter"] = monthly_df["month"].apply(
        lambda d: f"{d.year}-Q{(d.month - 1) // 3 + 1}"
    )
    quarterly = (
        monthly_df.groupby("quarter")
        .agg(avg_brier=("avg_brier_score", "mean"), n=("prediction_count", "sum"))
        .reset_index()
        .sort_values("quarter")
        .rename(columns={"quarter": "month"})
    )
    quarterly["rolling_avg"] = quarterly["avg_brier"].rolling(3, min_periods=1).mean()

    recent = bq.query(
        f"SELECT publish_date, home_team, away_team, "
        f"pred_home_win_pct, pred_draw_pct, pred_away_win_pct, "
        f"actual_result, brier_score "
        f"FROM `{MARTS}.fct_predictions` "
        f"ORDER BY publish_date DESC LIMIT 15"
    ).to_dataframe()

    all_brier = (
        bq.query(
            f"SELECT brier_score FROM `{MARTS}.fct_predictions` "
            f"WHERE brier_score IS NOT NULL"
        )
        .to_dataframe()["brier_score"]
        .tolist()
    )

    avg_brier = summary["all_time_avg_brier"]
    std_brier = std_row["std_brier"]
    accuracy  = summary["accuracy_pct"]
    is_sf     = summary["is_superforecaster"]
    total     = summary["total_predictions"]

    return recent, avg_brier, std_brier, accuracy, is_sf, quarterly, comp, total, all_brier


# ── pure text helpers ─────────────────────────────────────────────────────────
def rule(char: str = "=") -> str:
    return char * W


def thin() -> str:
    return "-" * W


def bar(value: float, *, width: int = 16, max_val: float = 0.30,
        threshold: float = 0.20) -> str:
    filled  = min(width, int(value / max_val * width))
    thr_pos = int(threshold / max_val * width)
    row     = "█" * filled + "░" * (width - filled)
    splice  = "┼" if filled > thr_pos else "|"
    row     = row[:thr_pos] + splice + row[thr_pos + 1:]
    return row


_SPARKS = "▁▂▃▄▅▆▇█"


def spark(v: float, lo: float = 0.10, hi: float = 0.30) -> str:
    idx = int((v - lo) / (hi - lo) * (len(_SPARKS) - 1))
    return _SPARKS[max(0, min(idx, len(_SPARKS) - 1))]


def _vis_len(s: str) -> int:
    """Visual length of a string after stripping HTML tags."""
    return len(re.sub(r'<[^>]+>', '', s))


def colored_brier(v: float, fmt) -> str:
    if v > 0.20:
        return fmt.color(f"{v:.4f}", "magenta")
    return f"{v:.4f}"


def tsep(*col_widths: int, prefix: str = "  ") -> str:
    return prefix + "─┼─".join("─" * w for w in col_widths)


# ── fmt contract ──────────────────────────────────────────────────────────────
# Both backends provide an object with these methods/attrs:
#   fmt.bold(text)      -> str
#   fmt.underline(text) -> str
#   fmt.color(text, name)  -> str   # name: "blue"|"magenta"|"orange"
#   fmt.muted(text)     -> str
#   fmt.highlight(text) -> str
#   fmt.escape(text)    -> str      # HTML-escape or passthrough
#   fmt.lt              -> str      # "<" or "&lt;"
#   fmt.gt              -> str      # ">" or "&gt;"


# ── translations ──────────────────────────────────────────────────────────────
def build_translations(avg_brier, std_brier, fmt):
    _cons = {
        "en": {True: "low",    None: "medium", False: "high"},
        "es": {True: "baja",   None: "media",  False: "alta"},
    }
    _cons_key = True if std_brier < 0.08 else (False if std_brier > 0.14 else None)
    lt, gt = fmt.lt, fmt.gt

    return {
        "en": {
            "header": [
                "@mundomaldini is a Spanish football journalist who publishes",
                "weekly match predictions on YouTube. Predictions are evaluated",
                f'with {fmt.bold("Brier scores")} (lower = more accurate). A {fmt.bold("superforecaster")}',
                f"scores Brier {lt} 0.20. Naive baseline: 0.2222 (33% each H/D/A).",
            ],
            "sf_yes": (
                f'\U0001f3c6 {fmt.bold("SUPERFORECASTER?")}  {fmt.bold("[x] YES")}   [ ] no'
                f'     (Brier {avg_brier:.4f} {lt} {BENCHMARKS["threshold"]})'
            ),
            "sf_no": (
                f'\U0001f3c6 {fmt.bold("SUPERFORECASTER?")}  [ ] yes   {fmt.bold("[x] NO")}'
                f'      (Brier {avg_brier:.4f} {gt} {BENCHMARKS["threshold"]})'
            ),
            "scored":      "Scored predictions",
            "accuracy":    "Accuracy          ",
            "brier":       "All-time Brier    ",
            "std":         "Std deviation     ",
            "consistency": f'{_cons["en"][_cons_key]} consistency',
            "benchmarks":  "Benchmarks:",
            "naive":       "Naive baseline",
            "markets":     "Betting markets",
            "maldini":     "Maldini        ",
            "comp_title":  "Competition breakdown  (sorted by Brier, | marks 0.20):",
            "comp_col":    "Competition",
            "qtr_title":   "Quarterly scores  (| marks 0.20 threshold):",
            "qtr_col":     "Quarter",
            "trend":       "trend",
            "improving":   fmt.color("improving \u2193", "blue"),
            "worsening":   fmt.color("worsening \u2191", "magenta"),
            "rec_title":   "Recent predictions  (last 15, all scored):",
            "date":        "Date",
            "home":        "Home",
            "away":        "Away",
            "probs":       "H/D/A%",
            "def_title":   "Definitions",
            "def_rows": [
                ("Brier score",    "Probabilistic accuracy metric. Lower = better."),
                ("",               f"Formula: (pH-rH)^2 + (pD-rD)^2 + (pA-rA)^2"),
                ("",               "p = predicted prob.; r = 1 if outcome, 0 if not."),
                ("",               "Range 0-2. Perfect = 0. Naive baseline = 0.2222."),
                None,
                ("Superforecaster",f"Avg Brier {lt} 0.20 over 100+ predictions."),
                ("",               "Concept: Philip Tetlock's Good Judgment Project."),
                None,
                ("Naive baseline", f"0.2222 — equal 1/3 to each outcome (H/D/A)."),
                ("Betting markets",f"~0.19 — wisdom of crowds (aggregated market odds)."),
                None,
                ("Avg Brier",      "Mean Brier score across all scored predictions."),
                ("Accuracy %",     "% where top predicted outcome == actual result."),
                ("Scored",         "Result confirmed in TheSportsDB; match played."),
            ],
            "colors":      "Colors:",
            "last_upd":    "last updated",
            "author":      "author      ",
            "dist_title":  "Brier distribution  (n={n}, mean={mean}, std={std}):",
            "dist_std_lo": "mean\u2500\u03c3",
            "dist_mean":   "mean",
            "dist_std_hi": "mean+\u03c3",
        },
        "es": {
            "header": [
                "@mundomaldini es un periodista español de fútbol que publica",
                f"predicciones semanales en YouTube. Se evalúan con {fmt.bold('puntuaciones')}",
                f'{fmt.bold("Brier")} (menor = más preciso). Un {fmt.bold("superpronosticador")} puntúa',
                f"Brier {lt} 0.20. Base aleatoria: 0.2222 (33% cada L/E/V).",
            ],
            "sf_yes": (
                f'\U0001f3c6 {fmt.bold("¿SUPERPRONOSTICADOR?")}  {fmt.bold("[x] SÍ")}   [ ] no'
                f'   (Brier {avg_brier:.4f} {lt} {BENCHMARKS["threshold"]})'
            ),
            "sf_no": (
                f'\U0001f3c6 {fmt.bold("¿SUPERPRONOSTICADOR?")}  [ ] sí   {fmt.bold("[x] NO")}'
                f'    (Brier {avg_brier:.4f} {gt} {BENCHMARKS["threshold"]})'
            ),
            "scored":      "Predicciones      ",
            "accuracy":    "Precisión         ",
            "brier":       "Brier histórico   ",
            "std":         "Desv. típica      ",
            "consistency": f'consistencia {_cons["es"][_cons_key]}',
            "benchmarks":  "Referencias:",
            "naive":       "Base aleatoria",
            "markets":     "Mercados apuestas",
            "maldini":     "Maldini        ",
            "comp_title":  "Desglose por competición  (ordenado por Brier, | = 0.20):",
            "comp_col":    "Competición",
            "qtr_title":   "Puntuaciones trimestrales  (| marca umbral 0.20):",
            "qtr_col":     "Trimestre",
            "trend":       "tendencia",
            "improving":   fmt.color("mejorando \u2193", "blue"),
            "worsening":   fmt.color("empeorando \u2191", "magenta"),
            "rec_title":   "Predicciones recientes  (últimas 15, todas puntuadas):",
            "date":        "Fecha",
            "home":        "Local",
            "away":        "Visitante",
            "probs":       "L/E/V%",
            "def_title":   "Definiciones",
            "def_rows": [
                ("Puntuación Brier", "Métrica de precisión probabilística. Menor = mejor."),
                ("",                 "Fórmula: (pH-rH)^2 + (pD-rD)^2 + (pA-rA)^2"),
                ("",                 "p = prob. predicha; r = 1 si ocurre, 0 si no."),
                ("",                 "Rango 0-2. Perfecto = 0. Base aleatoria = 0.2222."),
                None,
                ("Superpronosticador",f"Brier medio {lt} 0.20 en 100+ predicciones."),
                ("",                 "Concepto: Good Judgment Project (Philip Tetlock)."),
                None,
                ("Base aleatoria",   "0.2222 — 1/3 igual a cada resultado (L/E/V)."),
                ("Mercados apuestas","~0.19 — sabiduría colectiva (cuotas de mercado)."),
                None,
                ("Brier medio",      "Brier medio sobre todas las predicciones puntuadas."),
                ("Precisión %",      "% donde el resultado más probable coincide con real."),
                ("Puntuada",         "Resultado confirmado en TheSportsDB; partido jugado."),
            ],
            "colors":      "Colores:",
            "last_upd":    "actualizado    ",
            "author":      "autor          ",
            "dist_title":  "Distribución Brier  (n={n}, media={mean}, desv={std}):",
            "dist_std_lo": "media\u2500\u03c3",
            "dist_mean":   "media",
            "dist_std_hi": "media+\u03c3",
        },
    }


# ── section builder ───────────────────────────────────────────────────────────
def build_sections(T, fmt, df_scored, avg_brier, std_brier, accuracy,
                   is_sf, monthly, comp, total, all_brier):
    """Build all report sections as lists of formatted strings.

    Returns a dict: section_name -> list[str].
    """
    gt = fmt.gt
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── header ────────────────────────────────────────────────────────────
    header = [
        rule(),
        " " * ((W - 13) // 2) + fmt.bold("MALDINI STATS"),
        rule(),
        *T["header"],
        rule(),
    ]

    # ── summary ───────────────────────────────────────────────────────────
    sf_box = T["sf_yes"] if is_sf else T["sf_no"]
    _stat_lines = [
        f'{T["scored"]}: {fmt.bold(str(total))}',
        f'{T["accuracy"]}: {fmt.bold(f"{accuracy}%")}',
        f'{T["brier"]}: {fmt.bold(colored_brier(avg_brier, fmt))}',
        f'{T["std"]}: {std_brier:.3f}',
    ]
    _bm_lines = [
        T["benchmarks"],
        f'  {T["naive"]:<17}: {BENCHMARKS["naive_baseline"]}',
        f'  {T["markets"]:<17}: {BENCHMARKS["bookmaker"]}',
        f'  {T["maldini"]:<17}: {fmt.bold(f"{avg_brier:.4f}")}',
    ]
    _col_w = 30
    summary = [
        fmt.highlight(sf_box),
        thin(),
        *[f'{s}{" " * (_col_w - _vis_len(s))} | {b}' for s, b in zip(_stat_lines, _bm_lines)],
        rule(),
    ]

    # ── distribution ──────────────────────────────────────────────────────
    _BIN_W  = 0.025
    _D_LO, _D_HI = 0.0, 0.375
    _N_BINS = int((_D_HI - _D_LO) / _BIN_W)

    _dcounts = [0] * _N_BINS
    for _v in all_brier:
        if _v is not None:
            _i = min(_N_BINS - 1, max(0, int((_v - _D_LO) / _BIN_W)))
            _dcounts[_i] += 1
    while _dcounts and _dcounts[-1] == 0:
        _dcounts.pop()

    _n_all   = len(all_brier)
    _max_cnt = max(_dcounts) if _dcounts else 1
    _DBAR_W  = 24
    _actual_hi = _D_LO + len(_dcounts) * _BIN_W
    _ruler_hi  = max(_actual_hi, avg_brier + std_brier + _BIN_W)
    _RULER_W   = W - 4

    def _rpos(v):
        return max(0, min(_RULER_W - 1,
                          round((v - _D_LO) / (_ruler_hi - _D_LO) * (_RULER_W - 1))))

    def _place(arr, ctr, text):
        s = max(0, min(ctr - len(text) // 2, len(arr) - len(text)))
        for _j, _c in enumerate(text):
            arr[s + _j] = _c

    _dist_title = T["dist_title"].format(
        n=_n_all, mean=f"{avg_brier:.4f}", std=f"{std_brier:.3f}"
    )
    distribution = [fmt.bold(_dist_title), thin()]
    for _i, _cnt in enumerate(_dcounts):
        _lo_v   = _D_LO + _i * _BIN_W
        _hi_v   = _lo_v + _BIN_W
        _filled = int(_cnt / _max_cnt * _DBAR_W)
        _b      = "█" * _filled + "░" * (_DBAR_W - _filled)
        _pct    = _cnt / _n_all * 100 if _n_all > 0 else 0
        distribution.append(
            f"  {_lo_v:.3f}\u2500{_hi_v:.3f} \u2502 {_cnt:>4} ({_pct:4.1f}%) {_b}"
        )

    _slo_p = _rpos(avg_brier - std_brier)
    _m_p   = _rpos(avg_brier)
    _shi_p = _rpos(avg_brier + std_brier)
    _ruler = list("\u2500" * _RULER_W)
    _ruler[_slo_p] = "["
    _ruler[_m_p]   = "\u256a"
    _ruler[_shi_p] = "]"
    distribution.append(f"  {''.join(_ruler)}")

    _vals  = list(" " * _RULER_W)
    _names = list(" " * _RULER_W)
    _place(_vals,  _slo_p, f"{avg_brier - std_brier:.3f}")
    _place(_vals,  _m_p,   f"{avg_brier:.4f}")
    _place(_vals,  _shi_p, f"{avg_brier + std_brier:.3f}")
    _place(_names, _slo_p, T["dist_std_lo"])
    _place(_names, _m_p,   T["dist_mean"])
    _place(_names, _shi_p, T["dist_std_hi"])
    distribution.append(f"  {''.join(_vals)}")
    distribution.append(f"  {''.join(_names)}")
    distribution.append(rule())

    # ── competition breakdown ─────────────────────────────────────────────
    competition = [
        fmt.bold(T["comp_title"]),
        f'  {T["comp_col"]:<22} │ {"Brier":>6} │ {"Bar(|=.20)":<16} │ n (%)',
        thin(),
    ]
    for _, row in comp.iterrows():
        brier = row["avg_brier"]
        n     = int(row["n"])
        pct   = n / total * 100
        name  = fmt.escape(row["competition"])[:22]
        b     = bar(brier, width=16)
        competition.append(
            f"  {name:<22} │ {brier:.4f} │ {b} │ {n} ({pct:.1f}%)"
        )
    competition.append(rule())

    # ── quarterly scores ──────────────────────────────────────────────────
    spark_str = "".join(spark(m["avg_brier"]) for _, m in monthly.iterrows())
    q_vals    = monthly["avg_brier"].tolist()
    recent_3  = q_vals[-3:] if len(q_vals) >= 3 else q_vals
    trend_label = T["improving"] if recent_3[-1] < recent_3[0] else T["worsening"]

    quarterly = [
        fmt.bold(T["qtr_title"]),
        f'  {T["qtr_col"]:<9} │ {"Brier":>6} │ {"Bar":<20} │ n (%total)',
        thin(),
    ]
    for _, m in monthly.iterrows():
        b   = bar(m["avg_brier"], width=20)
        pct = m["n"] / total * 100
        quarterly.append(
            f"  {m['month']:<9} │ {m['avg_brier']:.4f} │ {b} │"
            f" {int(m['n'])} ({pct:.1f}%)"
        )
    quarterly.append(
        f'  {fmt.muted(f"{T['trend']} {spark_str}  {trend_label}")}'
    )
    quarterly.append(rule())

    # ── recent predictions ────────────────────────────────────────────────
    recent = [
        fmt.bold(T["rec_title"]),
        f'  {T["date"]:<10} │ {T["home"]:<{HW}} │ {T["away"]:<{AW}} │ {T["probs"]:<8} │ R │ Brier',
        thin(),
    ]
    for _, row in df_scored.iterrows():
        probs = f"{int(row.pred_home_win_pct)}/{int(row.pred_draw_pct)}/{int(row.pred_away_win_pct)}"
        home  = fmt.escape(str(row.home_team))[:HW]
        away  = fmt.escape(str(row.away_team))[:AW]
        recent.append(
            f"  {fmt.escape(str(row.publish_date)):<10} │ "
            f"{home:<{HW}} │ "
            f"{away:<{AW}} │ "
            f"{probs:<8} │ "
            f"{fmt.escape(str(row.actual_result)):<1} │ "
            f"{colored_brier(row.brier_score, fmt)}"
        )
    recent.append(rule())

    # ── definitions ───────────────────────────────────────────────────────
    definitions = [fmt.bold(T["def_title"]), thin()]
    for row in T["def_rows"]:
        if row is None:
            definitions.append("")
        else:
            term, defn = row
            if term:
                padded = fmt.underline(term) + " " * (15 - len(term))
            else:
                padded = " " * 15
            definitions.append(f"  {padded}: {defn}")
    _colors_term = T["colors"].rstrip(":")
    _colors_padded = fmt.underline(_colors_term) + " " * (15 - len(_colors_term))
    definitions.append(
        f'  {_colors_padded}: {fmt.color("■", "magenta")} magenta = Brier {gt} 0.20 (summary)'
    )
    definitions.append("")

    # ── footer ────────────────────────────────────────────────────────────
    footer = [
        rule(),
        f'{T["last_upd"]}: {now}',
        f'{T["author"]}: tomas-ravalli',
        "\U0001f41f",
    ]

    return {
        "header":       header,
        "summary":      summary,
        "distribution": distribution,
        "competition":  competition,
        "quarterly":    quarterly,
        "recent":       recent,
        "definitions":  definitions,
        "footer":       footer,
    }
