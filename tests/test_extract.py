"""
Unit tests for extract.py.

External calls (Anthropic API) are mocked.
"""

import json
from unittest.mock import MagicMock

from extract import (
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
        "raw_quote": "Le doy un 45% al Madrid, 25% al empate, 30% al Barca",
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


def _client(response_text: str) -> MagicMock:
    client = MagicMock()
    content = MagicMock()
    content.text = response_text
    client.messages.create.return_value.content = [content]
    return client


class TestExtractPredictionsFromTranscript:
    def test_returns_parsed_predictions(self):
        client = _client(json.dumps(SAMPLE_PREDS))
        result = extract_predictions_from_transcript(client, VIDEO_ID, "transcript text")
        assert len(result) == 2
        assert result[0]["home_team"] == "Real Madrid"

    def test_strips_markdown_code_fence(self):
        fenced = f"```json\n{json.dumps(SAMPLE_PREDS)}\n```"
        client = _client(fenced)
        assert len(extract_predictions_from_transcript(client, VIDEO_ID, "x")) == 2

    def test_returns_empty_list_on_invalid_json(self):
        assert extract_predictions_from_transcript(_client("not json"), VIDEO_ID, "x") == []

    def test_returns_empty_list_when_not_a_list(self):
        assert extract_predictions_from_transcript(
            _client(json.dumps({"error": "oops"})), VIDEO_ID, "x"
        ) == []

    def test_truncates_transcript_to_50k_chars(self):
        client = _client(json.dumps([]))
        extract_predictions_from_transcript(client, VIDEO_ID, "x" * 100_000)
        user_content = client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert len(user_content) <= 50_000 + 100

    def test_calls_haiku_model(self):
        client = _client(json.dumps([]))
        extract_predictions_from_transcript(client, VIDEO_ID, "text")
        assert "haiku" in client.messages.create.call_args.kwargs["model"]


class TestBuildPredictionRows:
    def test_prediction_id_format(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["prediction_id"] == f"{VIDEO_ID}_000"
        assert rows[1]["prediction_id"] == f"{VIDEO_ID}_001"

    def test_pred_sum_computed(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["pred_sum"] == 100
        assert rows[1]["pred_sum"] == 100

    def test_pred_sum_ok_true(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["pred_sum_ok"] is True

    def test_pred_sum_ok_false(self):
        bad = [{**SAMPLE_PREDS[0], "pred_away_win_pct": 10}]  # sum = 80
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, bad, PUBLISH_DATE)
        assert rows[0]["pred_sum_ok"] is False
        assert rows[0]["pred_sum"] == 80

    def test_match_type_knockout_when_no_draw(self):
        ucl = [{
            **SAMPLE_PREDS[0],
            "competition": "UEFA Champions League",
            "pred_draw_pct": 0,
            "pred_home_win_pct": 60,
            "pred_away_win_pct": 40,
        }]
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, ucl, PUBLISH_DATE)
        assert rows[0]["match_type"] == "knockout"

    def test_match_type_single_when_draw_present(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["match_type"] == "single"

    def test_match_date_none_when_empty_string(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[1]["match_date"] is None

    def test_match_date_preserved(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["match_date"] == "2025-03-15"

    def test_competition_defaults_to_unknown(self):
        pred = [{**SAMPLE_PREDS[0]}]
        del pred[0]["competition"]
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, pred, PUBLISH_DATE)
        assert rows[0]["competition"] == "Unknown"

    def test_empty_predictions_returns_empty(self):
        assert build_prediction_rows(VIDEO_ID, VIDEO_URL, [], PUBLISH_DATE) == []

    def test_publish_date_stored(self):
        rows = build_prediction_rows(VIDEO_ID, VIDEO_URL, SAMPLE_PREDS, PUBLISH_DATE)
        assert rows[0]["publish_date"] == PUBLISH_DATE
