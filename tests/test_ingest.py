"""
Unit tests for ingest.py.

External calls (YouTube API, youtube-transcript-api) are mocked.
"""

from unittest.mock import MagicMock, patch

from maldini.ingest import (
    fetch_yt_metadata,
    fetch_yt_transcript,
    parse_video_id,
)

MALDINI_VIDEO_ID = "gUw9BMrC0-Y"
MALDINI_URL = f"https://www.youtube.com/watch?v={MALDINI_VIDEO_ID}"

FAKE_METADATA = {
    "publish_date": "2025-03-10",
    "title": "Maldini: Pronosticos LaLiga Jornada 28",
}


def _snippet(text):
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


def _yt_mock(response=FAKE_YT_API_RESPONSE):
    yt = MagicMock()
    yt.videos.return_value.list.return_value.execute.return_value = response
    return yt


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

    def test_none_returns_none(self):
        assert parse_video_id(None) is None


class TestFetchYtMetadata:
    def test_returns_publish_date_and_title(self):
        yt = _yt_mock()
        assert fetch_yt_metadata(yt, MALDINI_VIDEO_ID) == FAKE_METADATA

    def test_truncates_timestamp_to_date(self):
        yt = _yt_mock()
        assert fetch_yt_metadata(yt, MALDINI_VIDEO_ID)["publish_date"] == "2025-03-10"

    def test_video_not_found_returns_none(self):
        yt = _yt_mock(response={"items": []})
        assert fetch_yt_metadata(yt, "nonexistent") is None

    def test_calls_api_with_correct_video_id(self):
        yt = _yt_mock()
        fetch_yt_metadata(yt, MALDINI_VIDEO_ID)
        yt.videos.return_value.list.assert_called_once_with(
            part="snippet", id=MALDINI_VIDEO_ID
        )


class TestFetchYtTranscript:
    @patch("maldini.ingest.YouTubeTranscriptApi")
    def test_joins_segments_into_text(self, MockYTApi):
        MockYTApi.return_value.fetch.return_value = [_snippet(t) for t in FAKE_SNIPPET_TEXTS]
        assert fetch_yt_transcript(MALDINI_VIDEO_ID) == FAKE_TRANSCRIPT_TEXT

    @patch("maldini.ingest.YouTubeTranscriptApi")
    def test_tries_spanish_first(self, MockYTApi):
        MockYTApi.return_value.fetch.return_value = [_snippet(t) for t in FAKE_SNIPPET_TEXTS]
        fetch_yt_transcript(MALDINI_VIDEO_ID)
        MockYTApi.return_value.fetch.assert_called_once_with(
            MALDINI_VIDEO_ID, languages=["es", "en"]
        )

    @patch("maldini.ingest.YouTubeTranscriptApi")
    def test_returns_none_on_failure(self, MockYTApi):
        MockYTApi.return_value.fetch.side_effect = Exception("Transcripts disabled")
        assert fetch_yt_transcript(MALDINI_VIDEO_ID) is None
