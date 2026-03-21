"""
Unit tests for src/extractor/extract_predictions.py

External dependencies (Anthropic API, BigQuery) are mocked so tests run
offline with no credentials.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.extractor.extract_predictions import (
    build_prediction_rows,
    extract_predictions_from_transcript,
)

VIDEO_ID = "gUw9BMrC0-Y"
VIDEO_URL = f"https://www.youtube.com/watch?v={VIDEO_ID}"
PUBLISH_DATE = "2025-03-10"

SAMPLE_PREDS = [
    {
        "match_date": "2025-03-15",
        "home_team": "Real Madrid",
        "away_team": "Barcelona",
        "competition": "LaLiga",
        "pred_home_win_pct": 45,
        "pred_draw_pct": 25,
        "pred_away_win_pct": 30,
        "raw_quote": "Le doy un 45% al Madrid, 25% al empate, 30% al Barça",
    },
    {
        "match_date": "",
        "home_team": "Atletico Madrid",
        "away_team": "Sevilla",
        "competition": "LaLiga",
        "pred_home_win_pct": 50,
        "pred_draw_pct": 30,
        "pred_away_win_pct": 20,
        "raw_quote": "50 atletico, 30 empate, 20 sevilla",
    },
]


def _make_client(response_text: str) -> MagicMock:
    """Builds a mock Anthropic client returning the given text."""
    client = MagicMock()
    content = MagicMock()
    content.text = response_text
    client.messages.create.return_value.content = [content]
    return client


# ---------------------------------------------------------------------------
# extract_predictions_from_transcript
# ---------------------------------------------------------------------------

class TestExtractPredictionsFromTranscript:
    def test_returns_parsed_predictions(self):
        import json
        client = _make_client(json.dumps(SAMPLE_PREDS))
        result = extract_predictions_from_transcript(client, VIDEO_ID, "transcript text")
        assert len(result) == 2
        assert result[0]["home_team"] == "Real Madrid"

    def test_strips_markdown_code_fence(self):
        import json
        fenced = f"```json\n{json.dumps(SAMPLE_PREDS)}\n```"
        client = _make_client(fenced)
        result = extract_predictions_from_transcript(client, VIDEO_ID, "transcript text")
        assert len(result) == 2

    def test_returns_empty_list_on_invalid_json(self):
        client = _make_client("this is not json")
        result = extract_predictions_from_transcript(client, VIDEO_ID, "transcript text")
        assert result == []

    def test_returns_empty_list_when_response_is_not_list(self):
        import json
        client = _make_client(json.dumps({"error": "oops"}))
        result = extract_predictions_from_transcript(client, VIDEO_ID, "transcript text")
        assert result == []

    def test_truncates_transcript_to_50k_chars(self):
        import json
        client = _make_client(json.dumps([]))
        long_transcript = "x" * 100_000
        extract_predictions_from_transcript(client, VIDEO_ID, long_transcript)
        call_args = client.messages.create.call_args
        user_content = call_args.kwargs["messages"][0]["content"]
        # transcript in the user message should be at most 50k chars of transcript + prefix
        assert len(user_content) <= 50_000 + 100

    def test_calls_haiku_model(self):
        import json
        client = _make_client(json.dumps([]))
        extract_predictions_from_transcript(client, VIDEO_ID, "text")
        call_kwargs = client.messages.create.call_args.kwargs
        assert "haiku" in call_kwargs["model"]


# ---------------------------------------------------------------------------
# build_prediction_rows — pure function, no mocking needed
# ---------------------------------------------------------------------------

class TestBuildPredictionRows:
    def test_prediction_id_format(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["prediction_id"] == f"{VIDEO_ID}_000"
        assert rows[1]["prediction_id"] == f"{VIDEO_ID}_001"

    def test_pred_sum_computed(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["pred_sum"] == 100   # 45 + 25 + 30
        assert rows[1]["pred_sum"] == 100   # 50 + 30 + 20

    def test_pred_sum_ok_true_when_sum_is_100(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["pred_sum_ok"] is True

    def test_pred_sum_ok_false_when_sum_not_100(self):
        bad_preds = [{**SAMPLE_PREDS[0], "pred_away_win_pct": 10}]  # sum = 80
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, bad_preds, PUBLISH_DATE)
        assert rows[0]["pred_sum_ok"] is False
        assert rows[0]["pred_sum"] == 80

    def test_match_type_aggregate_for_ucl_no_draw(self):
        ucl_pred = [{
            **SAMPLE_PREDS[0],
            "competition": "UEFA Champions League",
            "pred_draw_pct": 0,
            "pred_home_win_pct": 60,
            "pred_away_win_pct": 40,
        }]
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, ucl_pred, PUBLISH_DATE)
        assert rows[0]["match_type"] == "aggregate"

    def test_match_type_single_for_ucl_with_draw(self):
        ucl_pred = [{**SAMPLE_PREDS[0], "competition": "UEFA Champions League"}]
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, ucl_pred, PUBLISH_DATE)
        assert rows[0]["match_type"] == "single"

    def test_match_type_single_for_lalliga(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["match_type"] == "single"

    def test_match_date_none_when_empty_string(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[1]["match_date"] is None  # second pred has match_date = ""

    def test_match_date_preserved_when_provided(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["match_date"] == "2025-03-15"

    def test_competition_defaults_to_unknown(self):
        pred = [{**SAMPLE_PREDS[0]}]
        del pred[0]["competition"]
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, pred, PUBLISH_DATE)
        assert rows[0]["competition"] == "Unknown"

    def test_empty_predictions_returns_empty_list(self):
        assert build_prediction_rows(VIDEO_ID, VIDEO_URL, [], PUBLISH_DATE) == []

    def test_publish_date_stored_on_row(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["publish_date"] == PUBLISH_DATE
