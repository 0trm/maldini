"""
Unit tests for the pending-retry and override helpers in pipeline.py.
"""

import pandas as pd

from maldini.pipeline import apply_override, pending_rows


def _row(pid, match_type="single", result="H", brier=0.1):
    return {
        "prediction_id": pid, "competition": "LaLiga", "match_type": match_type,
        "match_date": None, "publish_date": "2025-03-01",
        "pred_home_win_pct": 60, "pred_draw_pct": 0 if match_type == "knockout" else 20,
        "pred_away_win_pct": 40 if match_type == "knockout" else 20,
        "actual_result": result, "brier_score": brier,
    }


class TestPendingRows:
    def test_returns_unscored_and_mislabelled_knockouts_only(self):
        df = pd.DataFrame([
            _row("scored"),
            _row("pending", result=None, brier=None),
            _row("old_knockout_draw", match_type="knockout", result="D", brier=0.26),
            _row("league_draw", result="D", brier=0.2),
        ])
        assert [r["prediction_id"] for r in pending_rows(df)] == ["pending", "old_knockout_draw"]

    def test_missing_values_become_none(self):
        df = pd.DataFrame([_row("pending", result=None, brier=None)])
        row = pending_rows(df)[0]
        assert row["match_date"] is None and row["actual_result"] is None


class TestApplyOverride:
    def test_winner_labels_a_level_knockout(self):
        row = apply_override(_row("x", match_type="knockout", result=None, brier=None),
                             {"home_goals": 1, "away_goals": 1, "winner": "A"})
        assert row["actual_result"] == "A"

    def test_without_winner_result_comes_from_goals(self):
        row = apply_override(_row("x", result=None, brier=None), {"home_goals": 0, "away_goals": 2})
        assert row["actual_result"] == "A"
