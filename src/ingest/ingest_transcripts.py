"""
ingest_transcripts.py
---------------------
Reads CSV files from data/inbox/, enriches each row using the YouTube Data API
(video_id, publish_date, title), fetches the transcript via youtube-transcript-api,
then appends new rows to BigQuery raw.transcripts. Deletes the CSV on success.

If the transcript cannot be auto-fetched (captions disabled), the row is skipped
with a warning — no manual fallback.

CSV format:
    video_url    — required (only column needed)

Usage:
    python src/ingest/ingest_transcripts.py
    python src/ingest/ingest_transcripts.py --dry-run
    python src/ingest/ingest_transcripts.py --file data/inbox/transcripts.csv
"""

import argparse
import os
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

INBOX = ROOT / "data" / "inbox"
PROJECT = "maldinia"
TABLE = f"{PROJECT}.raw.transcripts"

VIDEO_ID_RE = re.compile(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})")


# ---------------------------------------------------------------------------
# YouTube helpers
# ---------------------------------------------------------------------------

def parse_video_id(url: str) -> str | None:
    m = VIDEO_ID_RE.search(url)
    return m.group(1) if m else None


def fetch_yt_metadata(yt, video_id: str) -> dict | None:
    """Returns dict with publish_date (date) and title, or None on failure."""
    resp = yt.videos().list(part="snippet", id=video_id).execute()
    items = resp.get("items", [])
    if not items:
        return None
    snippet = items[0]["snippet"]
    publish_date = snippet["publishedAt"][:10]  # "2025-03-15T..." → "2025-03-15"
    return {"publish_date": publish_date, "title": snippet["title"]}


def fetch_yt_transcript(video_id: str) -> str | None:
    """Tries Spanish first, then English. Returns joined text or None."""
    try:
        transcript = YouTubeTranscriptApi().fetch(video_id, languages=["es", "en"])
        return " ".join(s.text for s in transcript)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------

def load_existing_video_ids(bq: bigquery.Client) -> set[str]:
    rows = bq.query(f"SELECT DISTINCT video_id FROM `{TABLE}`").result()
    return {row.video_id for row in rows}


def insert_to_bq(bq: bigquery.Client, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce").dt.date
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq.load_table_from_dataframe(df, TABLE, job_config=job_config)
    job.result()


# ---------------------------------------------------------------------------
# Per-file processing
# ---------------------------------------------------------------------------

def enrich_row(yt, row: pd.Series) -> dict | None:
    """
    Returns an enriched dict ready for BQ, or None if the row should be skipped.
    Transcript is always fetched automatically; rows are skipped with a warning if unavailable.
    """
    url = str(row["video_url"]).strip()
    video_id = parse_video_id(url)
    if not video_id:
        print(f"  [SKIP] Cannot parse video_id from URL: {url}")
        return None

    meta = fetch_yt_metadata(yt, video_id)
    if not meta:
        print(f"  [SKIP] {video_id}: not found via YouTube API")
        return None

    print(f"  {video_id}  |  {meta['publish_date']}  |  {meta['title']}")

    transcript = fetch_yt_transcript(video_id)
    if not transcript:
        print(f"    [WARN] Transcript unavailable (captions disabled on this video) — skipping")
        return None

    snippet = transcript[:80].replace("\n", " ")
    print(f"    transcript [AUTO]: {snippet}…")

    return {
        "video_id": video_id,
        "video_url": url,
        "publish_date": meta["publish_date"],
        "transcript_text": transcript,
    }


def process_file(
    bq: bigquery.Client,
    yt,
    path: Path,
    existing_ids: set[str],
    dry_run: bool,
) -> int:
    print(f"\n→ {path.name}")
    df = pd.read_csv(path)

    if "video_url" not in df.columns:
        print(f"  [ERROR] Missing required column 'video_url' — skipping file")
        return 0

    rows_to_insert = []
    for _, row in df.iterrows():
        video_id = parse_video_id(str(row["video_url"]))
        if video_id and video_id in existing_ids:
            print(f"  [SKIP] {video_id} already in BQ")
            continue

        enriched = enrich_row(yt, row)
        if enriched:
            rows_to_insert.append(enriched)

    if not rows_to_insert:
        print("  Nothing new to insert.")
        return 0

    if dry_run:
        print(f"  [DRY RUN] Would insert {len(rows_to_insert)} row(s) — file not deleted")
        return 0

    insert_to_bq(bq, rows_to_insert)
    path.unlink()
    print(f"  ✓ Inserted {len(rows_to_insert)} row(s) → {TABLE}. File deleted.")
    return len(rows_to_insert)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Ingest transcripts into BigQuery raw.transcripts.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to BQ.")
    parser.add_argument("--file", type=Path, help="Target a specific CSV file.")
    args = parser.parse_args()

    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY not set in .env")

    bq = bigquery.Client(project=PROJECT)
    yt = build("youtube", "v3", developerKey=api_key)

    existing_ids = load_existing_video_ids(bq)
    print(f"✓ {len(existing_ids)} video_id(s) already in BQ")

    csv_files = [args.file] if args.file else sorted(INBOX.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {INBOX}")
        return

    total = 0
    for path in csv_files:
        total += process_file(bq, yt, path, existing_ids, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\n✓ Done. {total} total row(s) inserted → {TABLE}")


if __name__ == "__main__":
    main()
