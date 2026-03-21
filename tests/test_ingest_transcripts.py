"""
Unit tests for src/ingest/ingest_transcripts.py

External dependencies (YouTube API, YouTubeTranscriptApi, BigQuery) are mocked
so tests run offline with no credentials.

Fixture: real Maldini video https://www.youtube.com/watch?v=gUw9BMrC0-Y
  → video_id: gUw9BMrC0-Y
  → used to anchor realistic mock responses
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingest.ingest_transcripts import (
    enrich_row,
    fetch_yt_metadata,
    fetch_yt_transcript,
    parse_video_id,
    process_file,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MALDINI_VIDEO_ID = "gUw9BMrC0-Y"
MALDINI_URL = f"https://www.youtube.com/watch?v={MALDINI_VIDEO_ID}"

FAKE_METADATA = {
    "publish_date": "2025-03-10",
    "title": "Maldini: Pronosticos LaLiga Jornada 28",
}

def _make_snippet(text):
    s = MagicMock()
    s.text = text
    return s

FAKE_SNIPPET_TEXTS = ["Hola a todos bienvenidos", "a Mundo Maldini.", "Hoy analizamos LaLiga."]
FAKE_TRANSCRIPT_TEXT = "Hola a todos bienvenidos a Mundo Maldini. Hoy analizamos LaLiga."

FAKE_YT_API_RESPONSE = {
    "items": [{
        "snippet": {
            "publishedAt": "2025-03-10T18:00:00Z",
            "title": "Maldini: Pronosticos LaLiga Jornada 28",
        }
    }]
}


def make_yt_mock(response=FAKE_YT_API_RESPONSE):
    """Builds a mock YouTube API client that returns `response` on .execute()."""
    yt = MagicMock()
    yt.videos.return_value.list.return_value.execute.return_value = response
    return yt


# ---------------------------------------------------------------------------
# parse_video_id — pure function, no mocking needed
# ---------------------------------------------------------------------------

class TestParseVideoId:
    def test_standard_url(self):
        assert parse_video_id(MALDINI_URL) == MALDINI_VIDEO_ID

    def test_short_url(self):
        assert parse_video_id(f"https://youtu.be/{MALDINI_VIDEO_ID}") == MALDINI_VIDEO_ID

    def test_url_with_extra_params(self):
        url = f"https://www.youtube.com/watch?v={MALDINI_VIDEO_ID}&t=42s"
        assert parse_video_id(url) == MALDINI_VIDEO_ID

    def test_invalid_url_returns_none(self):
        assert parse_video_id("https://vimeo.com/123456") is None

    def test_empty_string_returns_none(self):
        assert parse_video_id("") is None


# ---------------------------------------------------------------------------
# fetch_yt_metadata
# ---------------------------------------------------------------------------

class TestFetchYtMetadata:
    def test_returns_publish_date_and_title(self):
        yt = make_yt_mock()
        result = fetch_yt_metadata(yt, MALDINI_VIDEO_ID)
        assert result == FAKE_METADATA

    def test_truncates_timestamp_to_date(self):
        yt = make_yt_mock()
        result = fetch_yt_metadata(yt, MALDINI_VIDEO_ID)
        assert result["publish_date"] == "2025-03-10"   # not "2025-03-10T18:00:00Z"

    def test_video_not_found_returns_none(self):
        yt = make_yt_mock(response={"items": []})
        assert fetch_yt_metadata(yt, "nonexistent") is None

    def test_calls_api_with_correct_video_id(self):
        yt = make_yt_mock()
        fetch_yt_metadata(yt, MALDINI_VIDEO_ID)
        yt.videos.return_value.list.assert_called_once_with(
            part="snippet", id=MALDINI_VIDEO_ID
        )


# ---------------------------------------------------------------------------
# fetch_yt_transcript
# ---------------------------------------------------------------------------

class TestFetchYtTranscript:
    @patch("src.ingest.ingest_transcripts.YouTubeTranscriptApi")
    def test_joins_segments_into_text(self, MockYTApi):
        snippets = [_make_snippet(t) for t in FAKE_SNIPPET_TEXTS]
        MockYTApi.return_value.fetch.return_value = snippets
        result = fetch_yt_transcript(MALDINI_VIDEO_ID)
        assert result == FAKE_TRANSCRIPT_TEXT

    @patch("src.ingest.ingest_transcripts.YouTubeTranscriptApi")
    def test_tries_spanish_first(self, MockYTApi):
        snippets = [_make_snippet(t) for t in FAKE_SNIPPET_TEXTS]
        MockYTApi.return_value.fetch.return_value = snippets
        fetch_yt_transcript(MALDINI_VIDEO_ID)
        MockYTApi.return_value.fetch.assert_called_once_with(MALDINI_VIDEO_ID, languages=["es", "en"])

    @patch("src.ingest.ingest_transcripts.YouTubeTranscriptApi")
    def test_returns_none_on_failure(self, MockYTApi):
        MockYTApi.return_value.fetch.side_effect = Exception("Transcripts disabled")
        assert fetch_yt_transcript(MALDINI_VIDEO_ID) is None


# ---------------------------------------------------------------------------
# enrich_row
# ---------------------------------------------------------------------------

class TestEnrichRow:
    def _row(self, url=MALDINI_URL):
        return pd.Series({"video_url": url})

    @patch("src.ingest.ingest_transcripts.fetch_yt_transcript", return_value=FAKE_TRANSCRIPT_TEXT)
    @patch("src.ingest.ingest_transcripts.fetch_yt_metadata", return_value=FAKE_METADATA)
    def test_auto_fetches_transcript(self, mock_meta, mock_transcript):
        result = enrich_row(make_yt_mock(), self._row())
        assert result["transcript_text"] == FAKE_TRANSCRIPT_TEXT
        assert result["video_id"] == MALDINI_VIDEO_ID
        assert result["publish_date"] == "2025-03-10"

    @patch("src.ingest.ingest_transcripts.fetch_yt_transcript", return_value=None)
    @patch("src.ingest.ingest_transcripts.fetch_yt_metadata", return_value=FAKE_METADATA)
    def test_returns_none_when_transcript_unavailable(self, mock_meta, mock_transcript):
        assert enrich_row(make_yt_mock(), self._row()) is None

    @patch("src.ingest.ingest_transcripts.fetch_yt_metadata", return_value=None)
    def test_returns_none_when_video_not_found(self, mock_meta):
        assert enrich_row(make_yt_mock(), self._row()) is None

    def test_returns_none_for_unparseable_url(self):
        row = pd.Series({"video_url": "https://vimeo.com/123"})
        assert enrich_row(make_yt_mock(), row) is None


# ---------------------------------------------------------------------------
# process_file
# ---------------------------------------------------------------------------

class TestProcessFile:
    def _write_csv(self, tmp_path: Path, content: str) -> Path:
        p = tmp_path / "inbox.csv"
        p.write_text(content)
        return p

    @patch("src.ingest.ingest_transcripts.insert_to_bq")
    @patch("src.ingest.ingest_transcripts.enrich_row")
    def test_inserts_new_rows_and_deletes_csv(self, mock_enrich, mock_insert, tmp_path):
        mock_enrich.return_value = {
            "video_id": MALDINI_VIDEO_ID,
            "video_url": MALDINI_URL,
            "publish_date": "2025-03-10",
            "transcript_text": FAKE_TRANSCRIPT_TEXT,
        }
        path = self._write_csv(tmp_path, f"video_url\n{MALDINI_URL}\n")
        count = process_file(MagicMock(), MagicMock(), path, existing_ids=set(), dry_run=False)
        assert count == 1
        assert not path.exists()   # deleted on success
        mock_insert.assert_called_once()

    @patch("src.ingest.ingest_transcripts.insert_to_bq")
    @patch("src.ingest.ingest_transcripts.enrich_row")
    def test_dry_run_does_not_insert_or_delete(self, mock_enrich, mock_insert, tmp_path):
        mock_enrich.return_value = {
            "video_id": MALDINI_VIDEO_ID, "video_url": MALDINI_URL,
            "publish_date": "2025-03-10", "transcript_text": FAKE_TRANSCRIPT_TEXT,
        }
        path = self._write_csv(tmp_path, f"video_url\n{MALDINI_URL}\n")
        count = process_file(MagicMock(), MagicMock(), path, existing_ids=set(), dry_run=True)
        assert count == 0
        assert path.exists()       # not deleted
        mock_insert.assert_not_called()

    @patch("src.ingest.ingest_transcripts.enrich_row")
    def test_skips_duplicate_video_ids(self, mock_enrich, tmp_path):
        path = self._write_csv(tmp_path, f"video_url\n{MALDINI_URL}\n")
        count = process_file(
            MagicMock(), MagicMock(), path,
            existing_ids={MALDINI_VIDEO_ID},
            dry_run=False,
        )
        assert count == 0
        mock_enrich.assert_not_called()

    def test_missing_video_url_column_returns_zero(self, tmp_path):
        path = self._write_csv(tmp_path, "other_column\nsome value\n")
        count = process_file(MagicMock(), MagicMock(), path, existing_ids=set(), dry_run=False)
        assert count == 0
