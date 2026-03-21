"""
External source assets — data that arrives outside of Dagster.

raw_transcripts is populated via src/ingest/ingest_transcripts.py:
  - User drops a CSV (video_url, optional transcript_text) into data/inbox/
  - Script fetches metadata from YouTube Data API v3 and transcript from
    youtube-transcript-api, then inserts to BigQuery raw.transcripts
It's declared here so the dependency graph is complete.
"""

import dagster as dg

raw_transcripts = dg.AssetSpec(
    key="raw_transcripts",
    description="Transcripts ingested via src/ingest/ingest_transcripts.py into BigQuery raw.transcripts",
    group_name="sources",
    kinds={"bigquery"},
)
