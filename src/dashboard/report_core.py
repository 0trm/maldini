"""
report_core.py — shared data, helpers, translations, and section builder
for the Maldini Stats dashboard.

Both terminal_app.py (Streamlit) and cli_app.py (CLI) import from here.
"""

import math
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

    avg_brier = summary["all_time_avg_brier"]
    std_brier = std_row["std_brier"]
    accuracy  = summary["accuracy_pct"]
    is_sf     = summary["is_superforecaster"]
    total     = summary["total_predictions"]

    return recent, avg_brier, std_brier, accuracy, is_sf, quarterly, comp, total


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


def gauge(value: float, fmt, *, width: int = 20, max_val: float = 0.30,
          threshold: float = 0.20) -> list[str]:
    """Horizontal gauge: ├████████░░░░░░░░░░┤ with threshold marker."""
    filled = min(width, int(value / max_val * width))
    thr_pos = int(threshold / max_val * width)
    row = list("█" * filled + "░" * (width - filled))
    # place threshold marker
    if 0 <= thr_pos < width:
        row[thr_pos] = "┊"
    gauge_str = "├" + "".join(row) + "┤"

    # scale labels aligned under the gauge
    scale = " " * 1  # under ├
    thr_label = ".20"
    end_label = f".{int(max_val * 100):02d}"
    # positions: 0 under first char, thr_pos under threshold, width under last
    scale_line = list(" " * (width + 2))  # +2 for ├ and ┤
    scale_line[0] = "0"
    # place .20 centered on thr_pos+1 (offset by ├)
    thr_start = max(1, thr_pos + 1 - len(thr_label) // 2)
    for i, ch in enumerate(thr_label):
        if thr_start + i < len(scale_line):
            scale_line[thr_start + i] = ch
    # place max label at end
    end_start = max(0, len(scale_line) - len(end_label))
    for i, ch in enumerate(end_label):
        if end_start + i < len(scale_line):
            scale_line[end_start + i] = ch

    return [gauge_str, "".join(scale_line)]


def vbar_chart(labels: list[str], values: list[float], fmt, *,
               threshold: float = 0.20, bar_w: int = 2, gap: int = 2,
               step: float = 0.02, threshold_label: str = "threshold") -> list[str]:
    """Vertical bar chart with Y-axis, threshold line, and colored bars."""
    if not values:
        return []

    # dynamic Y range: snap to step grid
    y_min = math.floor((min(values) - step) / step) * step
    y_max = math.ceil((max(values) + step) / step) * step
    y_min = max(0, y_min)

    # number of rows
    n_rows = round((y_max - y_min) / step)
    col_w = bar_w + gap  # each column occupies bar_w + gap chars
    chart_w = len(values) * col_w + gap  # total chart body width

    lines = []
    for row_i in range(n_rows, -1, -1):
        y_val = y_min + row_i * step
        is_threshold = abs(y_val - threshold) < step / 2

        # Y-axis label
        label = f"  {y_val:.2f} ┤" if row_i > 0 else f"       └"

        # build row content
        row_chars = []
        for vi, v in enumerate(values):
            bar_bottom = y_min
            bar_top = v
            cell_bottom = y_val
            cell_top = y_val + step

            if row_i == 0:
                # bottom axis line
                row_chars.append("─" * col_w)
            elif cell_bottom < bar_top and cell_top > bar_bottom and cell_bottom >= y_min:
                # this cell is within the bar
                bar_str = "█" * bar_w
                if v > threshold:
                    bar_str = fmt.color(bar_str, "bar")
                # pad with threshold line or spaces
                if is_threshold:
                    row_chars.append(bar_str + "·" * gap)
                else:
                    row_chars.append(bar_str + " " * gap)
            else:
                # empty cell
                if is_threshold:
                    row_chars.append("·" * col_w)
                else:
                    row_chars.append(" " * col_w)

        row_body = " " * gap + "".join(row_chars)

        if row_i == 0:
            # bottom axis
            row_body = "─" * (chart_w + gap)

        # append threshold label
        if is_threshold and row_i > 0:
            row_body = row_body.rstrip() + " " + threshold_label

        lines.append(label + row_body)

    # X-axis labels (two lines: quarter name, year)
    x_line1 = "        "  # align under chart
    x_line2 = "        "
    for lbl in labels:
        # expect format like "2023-Q1" → show "Q1" / "'23"
        parts = lbl.split("-") if "-" in lbl else [lbl, ""]
        if len(parts) == 2 and parts[1].startswith("Q"):
            q_part = parts[1]  # "Q1"
            y_part = f"'{parts[0][2:]}"  # "'23"
        else:
            q_part = lbl[:4]
            y_part = ""
        x_line1 += f"{q_part:<{col_w}}"
        x_line2 += f"{y_part:<{col_w}}"

    lines.append(x_line1.rstrip())
    lines.append(x_line2.rstrip())

    return lines


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
            "vbar_title":  "Quarterly Brier Scores",
            "best_qtr":    "Best quarter  ",
            "worst_qtr":   "Worst quarter ",
            "active_since":"Active since  ",
            "threshold_lbl":"threshold",
            "legend":       fmt.color("■", "bar") + f" {gt} 0.20",
            "colors":      "Colors:",
            "last_upd":    "last updated",
            "author":      "author      ",
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
            "vbar_title":  "Puntuaciones trimestrales Brier",
            "best_qtr":    "Mejor trimestre",
            "worst_qtr":   "Peor trimestre ",
            "active_since":"Activo desde   ",
            "threshold_lbl":"umbral",
            "legend":       fmt.color("■", "bar") + f" {gt} 0.20",
            "colors":      "Colores:",
            "last_upd":    "actualizado    ",
            "author":      "autor          ",
        },
    }


# ── section builder ───────────────────────────────────────────────────────────
def build_sections(T, fmt, df_scored, avg_brier, std_brier, accuracy,
                   is_sf, monthly, comp, total):
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

    # ── summary (gauge + two-column stats) ─────────────────────────────
    sf_box = T["sf_yes"] if is_sf else T["sf_no"]

    # derive best / worst quarter
    best_idx  = monthly["avg_brier"].idxmin()
    worst_idx = monthly["avg_brier"].idxmax()
    best_qtr  = monthly.loc[best_idx, "month"]
    worst_qtr = monthly.loc[worst_idx, "month"]
    first_qtr = monthly.iloc[0]["month"]

    # gauge lines
    g_lines = gauge(avg_brier, fmt)

    # two-column layout: left = Brier gauge, right = stats
    _col_w = 30
    left_lines = [
        f'  {T["brier"]}: {fmt.bold(colored_brier(avg_brier, fmt))}',
        f'    {g_lines[0]}',
        f'    {g_lines[1]}',
        "",
        f'  {T["std"]}: {std_brier:.3f}',
    ]
    right_lines = [
        f'{T["scored"]}: {fmt.bold(str(total))}',
        f'{T["accuracy"]}: {fmt.bold(f"{accuracy}%")}',
        f'{T["best_qtr"]}: {fmt.color(str(best_qtr), "blue")}',
        f'{T["worst_qtr"]}: {fmt.color(str(worst_qtr), "magenta")}',
        f'{T["active_since"]}: {first_qtr}',
    ]

    summary = [
        fmt.highlight(sf_box),
        thin(),
    ]
    for l_line, r_line in zip(left_lines, right_lines):
        pad = _col_w - _vis_len(l_line)
        summary.append(f'{l_line}{" " * max(1, pad)} {r_line}')

    # benchmarks below
    summary.append(thin())
    summary.append(f'  {T["benchmarks"]}')
    summary.append(f'    {T["naive"]:<17}: {BENCHMARKS["naive_baseline"]}')
    summary.append(f'    {T["markets"]:<17}: {BENCHMARKS["bookmaker"]}')
    summary.append(f'    {T["maldini"]:<17}: {fmt.bold(f"{avg_brier:.4f}")}')
    summary.append(rule())

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
            f"  {name:<22} │ {colored_brier(brier, fmt)} │ {b} │ {n} ({pct:.1f}%)"
        )
    competition.append(rule())

    # ── quarterly scores (vertical bar chart) ──────────────────────────
    spark_str = "".join(spark(m["avg_brier"]) for _, m in monthly.iterrows())
    q_vals    = monthly["avg_brier"].tolist()
    q_labels  = monthly["month"].tolist()
    recent_3  = q_vals[-3:] if len(q_vals) >= 3 else q_vals
    trend_label = T["improving"] if recent_3[-1] < recent_3[0] else T["worsening"]

    chart_lines = vbar_chart(
        [str(l) for l in q_labels], q_vals, fmt,
        threshold_label=T["threshold_lbl"],
    )

    quarterly = [
        fmt.bold(T["vbar_title"]),
        "",
        *chart_lines,
        "",
        f'  {T["legend"]}',
        f'  {fmt.muted(f"{T['trend']} {spark_str}  {trend_label}")}',
        rule(),
    ]

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
        f'  {_colors_padded}: {fmt.color("■", "magenta")} highlighted {gt} 0.20'
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
        "header":      header,
        "summary":     summary,
        "competition": competition,
        "quarterly":   quarterly,
        "recent":      recent,
        "definitions": definitions,
        "footer":      footer,
    }
