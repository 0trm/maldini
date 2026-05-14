"""
Unit tests for scoring.py.

Brier formula reference (lower = better, 0 = perfect):
- 3-way: ((p_home - I_home)^2 + (p_draw - I_draw)^2 + (p_away - I_away)^2) / 3
- 2-way (knockout, after renormalising home + away to sum to 1):
        ((p_home - I_home)^2 + (p_away - I_away)^2) / 2
"""

from maldini.scoring import (
    brier_2way,
    brier_3way,
    classify_match_type,
    compute_brier,
)


class TestBrier3Way:
    def test_perfect_home_call(self):
        # Predict 100% home; home wins -> Brier 0
        assert brier_3way(1.0, 0.0, 0.0, "H") == 0.0

    def test_worst_call(self):
        # Predict 100% home; away wins -> ((1-0)^2 + 0 + (0-1)^2) / 3 = 2/3
        assert brier_3way(1.0, 0.0, 0.0, "A") == round(2.0 / 3.0, 4)

    def test_naive_baseline(self):
        # Predict 1/3 each; any outcome -> ((1/3-1)^2 + 2*(1/3)^2) / 3 = 0.2222
        assert brier_3way(1 / 3, 1 / 3, 1 / 3, "H") == 0.2222

    def test_draw_indicator(self):
        # Predict 60% home; draw happens
        # ((0.6-0)^2 + (0.2-1)^2 + (0.2-0)^2) / 3 = (0.36 + 0.64 + 0.04) / 3
        assert brier_3way(0.6, 0.2, 0.2, "D") == round((0.36 + 0.64 + 0.04) / 3.0, 4)


class TestBrier2Way:
    def test_perfect_knockout(self):
        # Predict 80% home (already implies 20% away); home wins
        # ph = 0.8, pa = 0.2 -> ((0.8-1)^2 + (0.2-0)^2) / 2 = 0.04
        assert brier_2way(0.8, 0.2, "H") == 0.04

    def test_renormalises(self):
        # pred percentages 60/40 of remainder after dropping draw=0
        # Already sum to 1, brier = ((0.6-1)^2 + (0.4-0)^2) / 2 = 0.16
        assert brier_2way(0.6, 0.4, "H") == 0.16

    def test_renormalises_when_under_one(self):
        # If the inputs are 0.3 and 0.2 (sum 0.5), renormalise to 0.6 and 0.4
        assert brier_2way(0.3, 0.2, "H") == 0.16

    def test_zero_sum_returns_zero(self):
        # Defensive: both zero shouldn't divide by zero
        assert brier_2way(0.0, 0.0, "H") == 0.0


class TestClassifyMatchType:
    def test_knockout_when_no_draw(self):
        assert classify_match_type(0) == "knockout"

    def test_single_when_draw_present(self):
        assert classify_match_type(25) == "single"


class TestComputeBrier:
    def test_returns_none_for_pending(self):
        row = {
            "pred_home_win_pct": 45, "pred_draw_pct": 25, "pred_away_win_pct": 30,
            "match_type": "single", "actual_result": None,
        }
        assert compute_brier(row) is None

    def test_dispatches_to_3way(self):
        row = {
            "pred_home_win_pct": 100, "pred_draw_pct": 0, "pred_away_win_pct": 0,
            "match_type": "single", "actual_result": "H",
        }
        # Single match_type uses 3-way; predict 100% home, home wins -> 0
        assert compute_brier(row) == 0.0

    def test_dispatches_to_2way_for_knockout(self):
        row = {
            "pred_home_win_pct": 80, "pred_draw_pct": 0, "pred_away_win_pct": 20,
            "match_type": "knockout", "actual_result": "H",
        }
        assert compute_brier(row) == 0.04
