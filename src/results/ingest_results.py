"""
ingest_results.py
-----------------
Manually ingest match results into BigQuery raw.match_results for predictions
that TheSportsDB could not match automatically.

Workflow:
  1. python src/results/fetch_results.py --list-pending   ← find prediction_ids
  2. Fill data/inbox/results.csv with the scores
  3. python src/results/ingest_results.py --dry-run        ← preview
  4. python src/results/ingest_results.py                  ← insert + delete CSV

CSV format:
    prediction_id   — from --list-pending output           (required)
    home_goals      — integer                              (required)
    away_goals      — integer                              (required)
    match_date      — YYYY-MM-DD                           (optional: looked up from prediction)
    match_type      — "single" or "aggregate"              (optional: looked up from prediction;
                      provide to override if AI extraction got it wrong)

actual_result (H/D/A) is derived from goals automatically.
competition, home_team, away_team, match_type, match_date are looked up from raw.predictions_extracted.

Usage:
    python src/results/ingest_results.py
    python src/results/ingest_results.py --dry-run
    python src/results/ingest_results.py --file data/inbox/results.csv
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

INBOX = ROOT / "data" / "inbox"
PROJECT = "maldinia"
TABLE = f"{PROJECT}.raw.match_results"
REQUIRED_COLUMNS = {"prediction_id", "home_goals", "away_goals"}


def derive_result(home_goals: float, away_goals: float) -> str:
    if home_goals > away_goals:
        return "H"
    elif away_goals > home_goals:
        return "A"
    return "D"


def load_prediction_lookup(bq: bigquery.Client, prediction_ids: list[str]) -> dict[str, dict]:
    """Returns {prediction_id: {competition, home_team, away_team, match_type}} for the given IDs."""
    ids_sql = ", ".join(f"'{pid}'" for pid in prediction_ids)
    query = f"""
        SELECT prediction_id, competition, home_team, away_team, match_type, match_date
        FROM `{PROJECT}.raw.predictions_extracted`
        WHERE prediction_id IN ({ids_sql})
    """
    return {row.prediction_id: dict(row) for row in bq.query(query).result()}


def load_existing_result_ids(bq: bigquery.Client) -> set[str]:
    rows = bq.query(f"SELECT DISTINCT result_id FROM `{TABLE}`").result()
    return {row.result_id for row in rows}


def insert_to_bq(bq: bigquery.Client, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.date
    df["fetched_at"] = datetime.now(timezone.utc)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = bq.load_table_from_dataframe(df, TABLE, job_config=job_config)
    job.result()


def process_file(
    bq: bigquery.Client,
    path: Path,
    existing_result_ids: set[str],
    dry_run: bool,
) -> int:
    print(f"\n→ {path.name}")
    df = pd.read_csv(path)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        print(f"  [ERROR] Missing columns: {missing} — skipping file")
        return 0

    prediction_ids = df["prediction_id"].tolist()
    lookup = load_prediction_lookup(bq, prediction_ids)

    rows_to_insert = []
    for _, row in df.iterrows():
        pid = str(row["prediction_id"]).strip()
        result_id = f"{pid}_result"

        if result_id in existing_result_ids:
            print(f"  [SKIP] {pid} already has a result in BQ")
            continue

        if pid not in lookup:
            print(f"  [SKIP] {pid} not found in raw.predictions_extracted")
            continue

        pred = lookup[pid]
        home_goals = float(row["home_goals"])
        away_goals = float(row["away_goals"])
        actual_result = derive_result(home_goals, away_goals)

        # match_type: use CSV value if provided, else fall back to extracted value
        csv_match_type = str(row.get("match_type", "")).strip() if "match_type" in df.columns else ""
        match_type = csv_match_type if csv_match_type else pred.get("match_type", "single")
        if csv_match_type and csv_match_type != pred.get("match_type"):
            print(f"  [WARN] {pid}: match_type in CSV ('{csv_match_type}') differs from extracted ('{pred.get('match_type')}') — using CSV value")

        # match_date: use CSV value if provided, else fall back to prediction's match_date
        csv_date = row.get("match_date", "") if "match_date" in df.columns else ""
        csv_date = "" if pd.isna(csv_date) else str(csv_date).strip()
        match_date = csv_date or str(pred.get("match_date") or "")

        print(
            f"  {pid}  |  [{match_type}]  {pred['home_team']} {int(home_goals)}-{int(away_goals)} {pred['away_team']}"
            f"  →  {actual_result}  |  {match_date or 'no date'}"
        )

        rows_to_insert.append({
            "result_id":     result_id,
            "prediction_id": pid,
            "competition":   pred["competition"],
            "home_team":     pred["home_team"],
            "away_team":     pred["away_team"],
            "home_goals":    home_goals,
            "away_goals":    away_goals,
            "actual_result": actual_result,
            "match_date":    match_date or None,
        })

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


def main():
    parser = argparse.ArgumentParser(description="Manually ingest match results into BigQuery.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to BQ.")
    parser.add_argument("--file", type=Path, help="Target a specific CSV file.")
    args = parser.parse_args()

    bq = bigquery.Client(project=PROJECT)
    existing_result_ids = load_existing_result_ids(bq)
    print(f"✓ {len(existing_result_ids)} result(s) already in BQ")

    csv_files = [args.file] if args.file else sorted(INBOX.glob("results*.csv"))
    if not csv_files:
        print(f"No results CSV files found in {INBOX}")
        print("  Tip: name your file 'results.csv' or 'results_*.csv'")
        return

    total = 0
    for path in csv_files:
        total += process_file(bq, path, existing_result_ids, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\n✓ Done. {total} total result(s) inserted → {TABLE}")


if __name__ == "__main__":
    main()
