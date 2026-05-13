"""
scoring.py -- Brier score computation.

Two variants:
- 3-outcome (standard league match):
    ((p_home - I_home)^2 + (p_draw - I_draw)^2 + (p_away - I_away)^2) / 3
- 2-outcome (knockout, when pred_draw_pct == 0): renormalise home + away to
  sum to 1, then ((p_home - I_home)^2 + (p_away - I_away)^2) / 2

p_* are predicted probabilities (0-1), I_* are indicator variables
(1 if the outcome occurred, else 0).
"""

from __future__ import annotations


def brier_3way(p_home: float, p_draw: float, p_away: float, actual: str) -> float:
    i_home = 1.0 if actual == "H" else 0.0
    i_draw = 1.0 if actual == "D" else 0.0
    i_away = 1.0 if actual == "A" else 0.0
    return round(
        ((p_home - i_home) ** 2 + (p_draw - i_draw) ** 2 + (p_away - i_away) ** 2) / 3.0,
        4,
    )


def brier_2way(p_home: float, p_away: float, actual: str) -> float:
    total = p_home + p_away
    if total <= 0:
        return 0.0
    ph = p_home / total
    pa = p_away / total
    i_home = 1.0 if actual == "H" else 0.0
    i_away = 1.0 if actual == "A" else 0.0
    return round(((ph - i_home) ** 2 + (pa - i_away) ** 2) / 2.0, 4)


def classify_match_type(pred_draw_pct: int) -> str:
    """A prediction with no draw probability is treated as a knockout (2-outcome)."""
    return "knockout" if int(pred_draw_pct) == 0 else "single"


def compute_brier(row: dict) -> float | None:
    """Dispatch based on match_type. Returns None if the result is missing."""
    actual = row.get("actual_result")
    if actual not in ("H", "D", "A"):
        return None
    p_home = row["pred_home_win_pct"] / 100.0
    p_draw = row["pred_draw_pct"] / 100.0
    p_away = row["pred_away_win_pct"] / 100.0
    if row.get("match_type") == "knockout":
        return brier_2way(p_home, p_away, actual)
    return brier_3way(p_home, p_draw, p_away, actual)
