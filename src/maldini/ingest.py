"""
ingest.py -- fetch YouTube metadata and transcripts.

Pure-ish: external API calls to YouTube Data API v3 and youtube-transcript-api,
no database side effects. Consumed by pipeline.py.
"""

from __future__ import annotations

import re

from youtube_transcript_api import YouTubeTranscriptApi

VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})")


def parse_video_id(url: str) -> str | None:
    m = VIDEO_ID_RE.search(url or "")
    return m.group(1) if m else None


def fetch_yt_metadata(yt, video_id: str) -> dict | None:
    """Returns {'publish_date': 'YYYY-MM-DD', 'title': str} or None."""
    resp = yt.videos().list(part="snippet", id=video_id).execute()
    items = resp.get("items", [])
    if not items:
        return None
    snippet = items[0]["snippet"]
    return {
        "publish_date": snippet["publishedAt"][:10],
        "title": snippet["title"],
    }


def fetch_yt_transcript(video_id: str) -> str | None:
    """Spanish first, then English. Returns joined text or None when captions are off."""
    try:
        transcript = YouTubeTranscriptApi().fetch(video_id, languages=["es", "en"])
        return " ".join(s.text for s in transcript)
    except Exception:
        return None
