"""
Unit tests for results.py.

All tested functions are pure (no network calls). External APIs are not invoked.
"""

from maldini.results import (
    fetch_result,
    get_result_code,
    infer_season,
    match_aggregate_to_result,
    match_prediction_to_result,
    normalise,
    teams_match,
)


# ── normalise ────────────────────────────────────────────────────────────────
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

    def test_marsella(self):
        assert normalise("Marsella") == "marseille"

    def test_napoles(self):
        assert normalise("Nápoles") == "napoli"

    def test_francfort(self):
        assert normalise("Eintracht Francfort") == "eintracht frankfurt"

    def test_strips_specials(self):
        assert "-" not in normalise("Paris Saint-Germain")

    def test_collapses_spaces(self):
        assert "  " not in normalise("Real   Madrid")


# ── teams_match ───────────────────────────────────────────────────────────────
class TestTeamsMatch:
    def test_exact_match(self):
        assert teams_match("Real Madrid", "Real Madrid") is True

    def test_case_insensitive(self):
        assert teams_match("barcelona", "BARCELONA") is True

    def test_prefix_stripped(self):
        assert teams_match("FC Barcelona", "Barcelona") is True

    def test_substring_match(self):
        assert teams_match("Madrid", "Atletico Madrid") is True

    def test_no_match(self):
        assert teams_match("Barcelona", "Atletico Madrid") is False

    def test_alias_marsella(self):
        assert teams_match("Marsella", "Marseille") is True

    def test_alias_union(self):
        assert teams_match("Union San Giluas", "Union SaintGilloise") is True

    def test_alias_sporting(self):
        assert teams_match("Sporting Portugal", "Sporting CP") is True


# ── infer_season ──────────────────────────────────────────────────────────────
class TestInferSeason:
    def test_aug_or_later(self):
        assert infer_season("LaLiga", "2024-08-01") == "2024-2025"

    def test_jan_gives_prev_year(self):
        assert infer_season("LaLiga", "2025-01-15") == "2024-2025"

    def test_single_year_mundial(self):
        assert infer_season("Mundial", "2026-06-15") == "2026"

    def test_single_year_eurocopa(self):
        assert infer_season("Eurocopa", "2024-07-01") == "2024"

    def test_single_year_copa_america(self):
        assert infer_season("Copa América", "2024-06-20") == "2024"

    def test_euro_2020_override(self):
        assert infer_season("Eurocopa", "2021-07-01") == "2020"

    def test_invalid_date(self):
        result = infer_season("LaLiga", "not-a-date")
        assert "-" in result or result.isdigit()

    def test_empty_date(self):
        assert infer_season("LaLiga", "")


# ── get_result_code ───────────────────────────────────────────────────────────
class TestGetResultCode:
    def test_home_win(self):
        assert get_result_code(2, 0) == "H"

    def test_away_win(self):
        assert get_result_code(0, 1) == "A"

    def test_draw(self):
        assert get_result_code(1, 1) == "D"

    def test_high_scoring_home_win(self):
        assert get_result_code(5, 3) == "H"


def _api_match(home, away, hg, ag, date="2025-03-15"):
    return {
        "strHomeTeam": home, "strAwayTeam": away,
        "intHomeScore": str(hg), "intAwayScore": str(ag),
        "dateEvent": date, "strStatus": "Match Finished",
    }


# ── match_prediction_to_result ────────────────────────────────────────────────
class TestMatchPredictionToResult:
    BASE = {
        "home_team": "Real Madrid",
        "away_team": "Barcelona",
        "match_date": "2025-03-15",
        "publish_date": "2025-03-10",
        "match_type": "single",
    }

    def test_matches_correct_fixture(self):
        result = match_prediction_to_result(
            self.BASE, [_api_match("Real Madrid", "Barcelona", 2, 1)]
        )
        assert result is not None
        assert result["home_goals"] == 2
        assert result["away_goals"] == 1
        assert result["actual_result"] == "H"

    def test_returns_none_when_no_fixture(self):
        result = match_prediction_to_result(
            self.BASE, [_api_match("Atletico Madrid", "Sevilla", 1, 0)]
        )
        assert result is None

    def test_skips_wrong_date(self):
        result = match_prediction_to_result(
            self.BASE, [_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-04-01")]
        )
        assert result is None

    def test_one_day_tolerance(self):
        result = match_prediction_to_result(
            self.BASE, [_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-03-16")]
        )
        assert result is not None

    def test_no_date_uses_45_day_window(self):
        pred = {**self.BASE, "match_date": None}
        result = match_prediction_to_result(
            pred, [_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-03-20")]
        )
        assert result is not None

    def test_no_date_rejects_pre_publish(self):
        pred = {**self.BASE, "match_date": None}
        result = match_prediction_to_result(
            pred, [_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-03-05")]
        )
        assert result is None

    def test_no_date_rejects_beyond_45_days(self):
        pred = {**self.BASE, "match_date": None}
        result = match_prediction_to_result(
            pred, [_api_match("Real Madrid", "Barcelona", 2, 1, date="2025-05-15")]
        )
        assert result is None

    def test_missing_scores(self):
        m = _api_match("Real Madrid", "Barcelona", 2, 1)
        m["intHomeScore"] = None
        assert match_prediction_to_result(self.BASE, [m]) is None


# ── match_aggregate_to_result ─────────────────────────────────────────────────
def _ucl_pred(home="Real Madrid", away="Bayern"):
    return {
        "home_team": home, "away_team": away,
        "match_type": "knockout",
        "competition": "UEFA Champions League",
        "match_date": "", "publish_date": "2025-03-01",
    }


class TestMatchAggregateToResult:
    def test_home_advances(self):
        leg1 = _api_match("Real Madrid", "Bayern", 3, 1, "2025-02-18")
        leg2 = _api_match("Bayern", "Real Madrid", 2, 1, "2025-03-05")
        result = match_aggregate_to_result(_ucl_pred(), [leg1, leg2])
        assert result is not None
        assert result["home_goals"] == 4
        assert result["away_goals"] == 3
        assert result["actual_result"] == "H"

    def test_away_advances(self):
        leg1 = _api_match("Real Madrid", "Bayern", 0, 2, "2025-02-18")
        leg2 = _api_match("Bayern", "Real Madrid", 1, 0, "2025-03-05")
        result = match_aggregate_to_result(_ucl_pred(), [leg1, leg2])
        assert result["actual_result"] == "A"
        assert result["home_goals"] == 0
        assert result["away_goals"] == 3

    def test_tied_aggregate_returns_none(self):
        leg1 = _api_match("Real Madrid", "Bayern", 1, 0, "2025-02-18")
        leg2 = _api_match("Bayern", "Real Madrid", 1, 0, "2025-03-05")
        assert match_aggregate_to_result(_ucl_pred(), [leg1, leg2]) is None

    def test_missing_leg(self):
        leg1 = _api_match("Real Madrid", "Bayern", 2, 1, "2025-02-18")
        assert match_aggregate_to_result(_ucl_pred(), [leg1]) is None

    def test_match_date_taken_from_leg2(self):
        leg1 = _api_match("Real Madrid", "Bayern", 3, 1, "2025-02-18")
        leg2 = _api_match("Bayern", "Real Madrid", 0, 1, "2025-03-05")
        result = match_aggregate_to_result(_ucl_pred(), [leg1, leg2])
        assert result["match_date"] == "2025-03-05"


# ── fetch_result dispatcher ───────────────────────────────────────────────────
class TestFetchResult:
    def test_single_match_falls_through_to_single_leg(self):
        pred = {
            "home_team": "Real Madrid", "away_team": "Barcelona",
            "match_date": "2025-03-15", "publish_date": "2025-03-10",
            "match_type": "single", "competition": "LaLiga",
        }
        api = [_api_match("Real Madrid", "Barcelona", 2, 1)]
        result = fetch_result(pred, api)
        assert result["actual_result"] == "H"

    def test_knockout_ucl_prefers_aggregate(self):
        pred = _ucl_pred()
        leg1 = _api_match("Real Madrid", "Bayern", 3, 1, "2025-02-18")
        leg2 = _api_match("Bayern", "Real Madrid", 2, 1, "2025-03-05")
        result = fetch_result(pred, [leg1, leg2])
        assert result["home_goals"] == 4

    def test_knockout_outside_ucl_uel_uses_single_leg(self):
        pred = {
            "home_team": "Real Madrid", "away_team": "Barcelona",
            "match_date": "2025-04-12", "publish_date": "2025-04-01",
            "match_type": "knockout", "competition": "Supercopa de España",
        }
        api = [_api_match("Real Madrid", "Barcelona", 2, 1, "2025-04-12")]
        result = fetch_result(pred, api)
        assert result is not None
        assert result["home_goals"] == 2
