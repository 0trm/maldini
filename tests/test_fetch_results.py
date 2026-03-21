"""
Unit tests for src/results/fetch_results.py

All pure functions (normalise, teams_match, infer_season, get_result_code,
match_prediction_to_result, match_aggregate_to_result) are tested without
any network calls or BigQuery access.
"""

import pytest

from src.results.fetch_results import (
    get_result_code,
    infer_season,
    match_aggregate_to_result,
    match_prediction_to_result,
    normalise,
    teams_match,
)


# ---------------------------------------------------------------------------
# normalise
# ---------------------------------------------------------------------------

class TestNormalise:
    def test_lowercases(self):
        assert normalise("Real Madrid") == "madrid"

    def test_strips_accents(self):
        assert normalise("Atlético") == "atletico"

    def test_removes_real_prefix(self):
        assert normalise("Real Sociedad") == "sociedad"

    def test_removes_atletico_prefix(self):
        assert normalise("Atlético Madrid") == "madrid"

    def test_removes_fc_prefix(self):
        assert normalise("FC Barcelona") == "barcelona"

    def test_removes_club_prefix(self):
        assert normalise("Club Brugge") == "brugge"

    def test_removes_de_del(self):
        assert normalise("Recreativo de Huelva") == "recreativo huelva"

    def test_applies_word_substitution_marsella(self):
        assert normalise("Marsella") == "marseille"

    def test_applies_word_substitution_napoles(self):
        assert normalise("Nápoles") == "napoli"

    def test_applies_word_substitution_francfort(self):
        assert normalise("Eintracht Francfort") == "eintracht frankfurt"

    def test_strips_special_characters(self):
        # Apostrophes, hyphens etc. should be removed
        result = normalise("Paris Saint-Germain")
        assert "-" not in result

    def test_collapses_multiple_spaces(self):
        assert "  " not in normalise("Real   Madrid")


# ---------------------------------------------------------------------------
# teams_match
# ---------------------------------------------------------------------------

class TestTeamsMatch:
    def test_exact_match_after_normalisation(self):
        assert teams_match("Real Madrid", "Real Madrid") is True

    def test_case_insensitive(self):
        assert teams_match("barcelona", "BARCELONA") is True

    def test_prefix_stripped_match(self):
        assert teams_match("FC Barcelona", "Barcelona") is True

    def test_substring_match_pred_in_api(self):
        # "madrid" is in "atletico madrid"
        assert teams_match("Madrid", "Atletico Madrid") is True

    def test_no_match_different_teams(self):
        assert teams_match("Barcelona", "Atletico Madrid") is False

    def test_word_substitution_alias(self):
        assert teams_match("Marsella", "Marseille") is True

    def test_team_alias_union_saintgilloise(self):
        assert teams_match("Union San Giluas", "Union SaintGilloise") is True

    def test_sporting_portugal_alias(self):
        assert teams_match("Sporting Portugal", "Sporting CP") is True


# ---------------------------------------------------------------------------
# infer_season
# ---------------------------------------------------------------------------

class TestInferSeason:
    def test_aug_or_later_gives_current_next_season(self):
        assert infer_season("LaLiga", "2024-08-01") == "2024-2025"

    def test_jan_gives_prev_current_season(self):
        assert infer_season("LaLiga", "2025-01-15") == "2024-2025"

    def test_single_year_tournament_mundial(self):
        assert infer_season("Mundial", "2026-06-15") == "2026"

    def test_single_year_tournament_eurocopa(self):
        assert infer_season("Eurocopa", "2024-07-01") == "2024"

    def test_single_year_tournament_copa_america(self):
        assert infer_season("Copa América", "2024-06-20") == "2024"

    def test_season_override_euro_2020_played_in_2021(self):
        # Euro 2020 was played in 2021 but listed as "2020" in TheSportsDB
        assert infer_season("Eurocopa", "2021-07-01") == "2020"

    def test_invalid_date_falls_back_gracefully(self):
        # Should not raise; returns some valid season string
        result = infer_season("LaLiga", "not-a-date")
        assert "-" in result or result.isdigit()

    def test_empty_date_falls_back_gracefully(self):
        result = infer_season("LaLiga", "")
        assert result  # non-empty


# ---------------------------------------------------------------------------
# get_result_code
# ---------------------------------------------------------------------------

class TestGetResultCode:
    def test_home_win(self):
        assert get_result_code(2, 0) == "H"

    def test_away_win(self):
        assert get_result_code(0, 1) == "A"

    def test_draw(self):
        assert get_result_code(1, 1) == "D"

    def test_high_scoring_home_win(self):
        assert get_result_code(5, 3) == "H"


# ---------------------------------------------------------------------------
# match_prediction_to_result
# ---------------------------------------------------------------------------

def _make_api_match(home, away, home_goals, away_goals, date="2025-03-15"):
    return {
        "strHomeTeam": home,
        "strAwayTeam": away,
        "intHomeScore": str(home_goals),
        "intAwayScore": str(away_goals),
        "dateEvent": date,
        "strStatus": "Match Finished",
    }


class TestMatchPredictionToResult:
    BASE_PRED = {
        "home_team": "Real Madrid",
        "away_team": "Barcelona",
        "match_date": "2025-03-15",
        "publish_date": "2025-03-10",
        "match_type": "single",
    }

    def test_matches_correct_fixture(self):
        matches = [_make_api_match("Real Madrid", "Barcelona", 2, 1)]
        result = match_prediction_to_result(self.BASE_PRED, matches)
        assert result is not None
        assert result["actual_home_goals"] == 2
        assert result["actual_away_goals"] == 1
        assert result["actual_result"] == "H"

    def test_returns_none_when_no_fixture_found(self):
        matches = [_make_api_match("Atletico Madrid", "Sevilla", 1, 0)]
        result = match_prediction_to_result(self.BASE_PRED, matches)
        assert result is None

    def test_skips_fixture_with_wrong_date(self):
        matches = [_make_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-04-01")]
        result = match_prediction_to_result(self.BASE_PRED, matches)
        assert result is None

    def test_accepts_fixture_within_one_day_tolerance(self):
        matches = [_make_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-03-16")]
        result = match_prediction_to_result(self.BASE_PRED, matches)
        assert result is not None

    def test_no_date_uses_45_day_window(self):
        pred = {**self.BASE_PRED, "match_date": None, "publish_date": "2025-03-10"}
        matches = [_make_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-03-20")]
        result = match_prediction_to_result(pred, matches)
        assert result is not None

    def test_no_date_rejects_match_before_publish_date(self):
        pred = {**self.BASE_PRED, "match_date": None, "publish_date": "2025-03-10"}
        matches = [_make_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-03-05")]
        result = match_prediction_to_result(pred, matches)
        assert result is None

    def test_no_date_rejects_match_beyond_45_days(self):
        pred = {**self.BASE_PRED, "match_date": None, "publish_date": "2025-03-10"}
        matches = [_make_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-05-15")]
        result = match_prediction_to_result(pred, matches)
        assert result is None

    def test_returns_none_when_scores_missing(self):
        match = _make_api_match("Real Madrid", "Barcelona", 2, 1)
        match["intHomeScore"] = None
        result = match_prediction_to_result(self.BASE_PRED, [match])
        assert result is None


# ---------------------------------------------------------------------------
# match_aggregate_to_result
# ---------------------------------------------------------------------------

def _ucl_pred(home="Real Madrid", away="Bayern"):
    return {
        "home_team": home,
        "away_team": away,
        "match_type": "aggregate",
        "match_date": "",
        "publish_date": "2025-03-01",
    }


class TestMatchAggregateToResult:
    def test_correct_aggregate_home_wins(self):
        # Leg 1: Real Madrid 3-1 Bayern (at Madrid)
        # Leg 2: Bayern 2-1 Real Madrid (at Bayern) → agg 4-3 Real Madrid
        leg1 = _make_api_match("Real Madrid", "Bayern", 3, 1, "2025-02-18")
        leg2 = _make_api_match("Bayern", "Real Madrid", 2, 1, "2025-03-05")
        result = match_aggregate_to_result(_ucl_pred(), [leg1, leg2])
        assert result is not None
        assert result["actual_home_goals"] == 4  # Real Madrid total
        assert result["actual_away_goals"] == 3  # Bayern total
        assert result["actual_result"] == "H"

    def test_correct_aggregate_away_wins(self):
        # Leg 1: Real Madrid 0-2 Bayern; Leg 2: Bayern 1-0 Real Madrid → agg 1-3 Bayern wins
        leg1 = _make_api_match("Real Madrid", "Bayern", 0, 2, "2025-02-18")
        leg2 = _make_api_match("Bayern", "Real Madrid", 1, 0, "2025-03-05")
        result = match_aggregate_to_result(_ucl_pred(), [leg1, leg2])
        assert result["actual_result"] == "A"
        assert result["actual_home_goals"] == 0   # Real Madrid total
        assert result["actual_away_goals"] == 3   # Bayern total

    def test_tied_aggregate_returns_none(self):
        # Leg 1: Real Madrid 1-0 Bayern; Leg 2: Bayern 1-0 Real Madrid → agg 1-1, ET/pens
        leg1 = _make_api_match("Real Madrid", "Bayern", 1, 0, "2025-02-18")
        leg2 = _make_api_match("Bayern", "Real Madrid", 1, 0, "2025-03-05")
        result = match_aggregate_to_result(_ucl_pred(), [leg1, leg2])
        assert result is None

    def test_returns_none_when_leg_missing(self):
        leg1 = _make_api_match("Real Madrid", "Bayern", 2, 1, "2025-02-18")
        # No leg2
        result = match_aggregate_to_result(_ucl_pred(), [leg1])
        assert result is None

    def test_returns_none_on_missing_scores(self):
        leg1 = _make_api_match("Real Madrid", "Bayern", 2, 1)
        leg2 = _make_api_match("Bayern", "Real Madrid", 1, 0)
        leg1["intHomeScore"] = None
        result = match_aggregate_to_result(_ucl_pred(), [leg1, leg2])
        assert result is None

    def test_match_date_taken_from_leg2(self):
        leg1 = _make_api_match("Real Madrid", "Bayern", 3, 1, "2025-02-18")
        leg2 = _make_api_match("Bayern", "Real Madrid", 0, 1, "2025-03-05")
        result = match_aggregate_to_result(_ucl_pred(), [leg1, leg2])
        assert result["match_date"] == "2025-03-05"
