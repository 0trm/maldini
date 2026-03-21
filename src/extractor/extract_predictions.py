"""
extract_predictions.py
----------------------
Reads transcripts from BigQuery raw.transcripts, skips any video_id
already present in raw.predictions_extracted, and uses Claude Haiku
to extract match predictions. Appends new rows to raw.predictions_extracted.

Usage:
    python src/extractor/extract_predictions.py
    python src/extractor/extract_predictions.py --dry-run
    python src/extractor/extract_predictions.py --video-id XYZ
    python src/extractor/extract_predictions.py --video-id XYZ --force
"""

import anthropic
import json
import re
import argparse
from datetime import date
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

PROJECT = "maldinia"

AGGREGATE_COMPETITIONS = {"UEFA Champions League", "UEFA Europa League"}

SYSTEM_PROMPT = """You are a precise data extractor. Your job is to find football match predictions
in Spanish-language transcripts from the YouTube channel "Mundo Maldini".

Maldini always states predictions as percentages for three outcomes: home win, draw, away win.
Sometimes there is no draw (knockout phases, second legs) — in that case pred_draw_pct is 0.

Rules:
- Only extract predictions that have explicit percentage numbers.
- Ignore vague statements without percentages.
- Team names: use the canonical Spanish name as spoken (e.g. "Athletic Club", "Barça", "Real Madrid").
- competition: infer from context. One of: LaLiga, UEFA Champions League, UEFA Europa League, Copa del Rey,
  Supercopa de España, Supercopa de Europa, Mundial de Clubes, Mundial, UEFA Nations League,
  Copa América, Eurocopa, Copa de África. Use "Unknown" if unclear.
- match_date: extract if mentioned in the transcript (YYYY-MM-DD). Leave empty string if not mentioned.
- raw_quote: the verbatim sentence(s) containing the percentages.
- pred_sum: sum of the three percentages. pred_sum_ok: true if pred_sum == 100.

Return ONLY valid JSON — an array of prediction objects. No markdown, no explanation.
Schema per object:
{
  "match_date": "",
  "home_team": "",
  "away_team": "",
  "competition": "",
  "pred_home_win_pct": 0,
  "pred_draw_pct": 0,
  "pred_away_win_pct": 0,
  "raw_quote": ""
}"""


def load_existing_video_ids(bq: bigquery.Client) -> set[str]:
    rows = bq.query(
        f"SELECT DISTINCT video_id FROM `{PROJECT}.raw.predictions_extracted`"
    ).result()
    return {row.video_id for row in rows}


def load_transcripts(bq: bigquery.Client, only_video_id: str | None = None) -> list[dict]:
    if only_video_id:
        query = f"""
            SELECT video_id, video_url, publish_date, transcript_text
            FROM `{PROJECT}.raw.transcripts`
            WHERE video_id = '{only_video_id}'
        """
    else:
        query = f"""
            SELECT video_id, video_url, publish_date, transcript_text
            FROM `{PROJECT}.raw.transcripts`
        """
    rows = bq.query(query).result()
    return [dict(row) for row in rows]


def extract_predictions_from_transcript(
    client: anthropic.Anthropic,
    video_id: str,
    transcript: str,
) -> list[dict]:
    transcript_text = transcript[:50_000]

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"Extract all match predictions from this transcript:\n\n{transcript_text}",
            }
        ],
    )

    raw = message.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        predictions = json.loads(raw)
        if not isinstance(predictions, list):
            print(f"  [WARN] video {video_id}: expected list, got {type(predictions)}")
            return []
        return predictions
    except json.JSONDecodeError as e:
        print(f"  [ERROR] video {video_id}: JSON parse failed — {e}")
        print(f"  Raw response: {raw[:300]}")
        return []


def build_prediction_rows(
    video_id: str, video_url: str, raw_preds: list[dict], publish_date=None
) -> list[dict]:
    rows = []
    for i, pred in enumerate(raw_preds):
        home_pct = int(pred.get("pred_home_win_pct", 0))
        draw_pct = int(pred.get("pred_draw_pct", 0))
        away_pct = int(pred.get("pred_away_win_pct", 0))
        pred_sum = home_pct + draw_pct + away_pct

        rows.append({
            "prediction_id":    f"{video_id}_{i:03d}",
            "video_id":         video_id,
            "video_url":        video_url,
            "publish_date":     publish_date,
            "match_date":       pred.get("match_date") or None,
            "home_team":        pred.get("home_team", ""),
            "away_team":        pred.get("away_team", ""),
            "competition":      pred.get("competition", "Unknown"),
            "match_type":       (
                "aggregate"
                if pred.get("competition", "Unknown") in AGGREGATE_COMPETITIONS and draw_pct == 0
                else "single"
            ),
            "leg_number":       1,
            "pred_home_win_pct": home_pct,
            "pred_draw_pct":    draw_pct,
            "pred_away_win_pct": away_pct,
            "pred_sum":         pred_sum,
            "pred_sum_ok":      pred_sum == 100,
            "raw_quote":        pred.get("raw_quote", ""),
            "extracted_at":     date.today().isoformat(),
        })
    return rows


def insert_to_bq(bq: bigquery.Client, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    for col in ("publish_date", "match_date", "extracted_at"):
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq.load_table_from_dataframe(
        df, f"{PROJECT}.raw.predictions_extracted", job_config=job_config
    )
    job.result()


def main():
    parser = argparse.ArgumentParser(description="Extract Maldini predictions from transcripts.")
    parser.add_argument("--dry-run",  action="store_true", help="Print results without saving.")
    parser.add_argument("--video-id", type=str,            help="Process a single video ID only.")
    parser.add_argument("--force",    action="store_true", help="Re-extract even if already in BQ.")
    args = parser.parse_args()

    bq     = bigquery.Client(project=PROJECT)
    client = anthropic.Anthropic()

    existing_ids   = load_existing_video_ids(bq)
    all_transcripts = load_transcripts(bq, only_video_id=args.video_id)

    if args.force or args.video_id:
        to_process = all_transcripts
    else:
        to_process = [t for t in all_transcripts if t["video_id"] not in existing_ids]

    if not to_process:
        print("✓ Nothing to process — all transcripts already extracted.")
        return

    print(f"→ Processing {len(to_process)} video(s)  |  already extracted: {len(existing_ids)}")
    if args.dry_run:
        print("  [DRY RUN — results will not be saved]")

    total_new = 0
    for i, item in enumerate(to_process, 1):
        vid_id  = item["video_id"]
        vid_url = item["video_url"]
        print(f"\n[{i}/{len(to_process)}] {vid_id}  ({vid_url})")

        raw_preds = extract_predictions_from_transcript(client, vid_id, item["transcript_text"])
        rows      = build_prediction_rows(vid_id, vid_url, raw_preds, publish_date=item["publish_date"])

        print(f"  → {len(rows)} prediction(s) extracted")

        if args.dry_run:
            for r in rows:
                print(f"     {r['home_team']} vs {r['away_team']}  |  "
                      f"{r['pred_home_win_pct']}/{r['pred_draw_pct']}/{r['pred_away_win_pct']}  |  "
                      f"{r['competition']}")
        else:
            insert_to_bq(bq, rows)
            total_new += len(rows)

    if not args.dry_run:
        print(f"\n✓ Done. {total_new} new prediction row(s) inserted → raw.predictions_extracted")


if __name__ == "__main__":
    main()
