"""
Dagster assets wrapping the Python enrichment scripts.

These assets call external APIs (YouTube, Claude, TheSportsDB) and write to
BigQuery raw tables. They reuse the existing logic in src/ingest, src/extractor,
and src/results.
"""

import os
import sys
from pathlib import Path

import dagster as dg

# Add project root to path so we can import from src.*
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

INBOX = ROOT / "data" / "inbox"


@dg.asset(
    key="raw_transcripts",
    description="Ingest new video transcripts from data/inbox/ CSVs → BigQuery raw.transcripts",
    group_name="ingestion",
    kinds={"python", "bigquery"},
)
def ingest_transcripts(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    import os
    from google.cloud import bigquery
    from googleapiclient.discovery import build

    from src.ingest.ingest_transcripts import (
        load_existing_video_ids,
        process_file,
    )

    api_key = os.environ.get("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError("YOUTUBE_API_KEY not set")

    bq = bigquery.Client(project="maldinia")
    yt = build("youtube", "v3", developerKey=api_key)

    existing_ids = load_existing_video_ids(bq)
    csv_files = sorted(INBOX.glob("*.csv"))

    if not csv_files:
        context.log.info("No CSV files found in data/inbox/ — nothing to ingest.")
        return dg.MaterializeResult(metadata={"new_transcripts": 0})

    total = 0
    for path in csv_files:
        context.log.info(f"Processing {path.name}")
        n = process_file(bq, yt, path, existing_ids, dry_run=False)
        total += n
        context.log.info(f"  {n} transcript(s) inserted from {path.name}")

    return dg.MaterializeResult(metadata={"new_transcripts": total})


@dg.asset(
    key="raw_predictions_extracted",
    deps=[dg.AssetKey("raw_transcripts")],
    description="Extract predictions from new transcripts via Claude Haiku → raw.predictions_extracted",
    group_name="ingestion",
    kinds={"python", "bigquery"},
)
def extract_predictions(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    from google.cloud import bigquery
    import anthropic

    from src.extractor.extract_predictions import (
        load_existing_video_ids,
        load_transcripts,
        extract_predictions_from_transcript,
        build_prediction_rows,
        insert_to_bq,
    )

    bq = bigquery.Client(project="maldinia")
    client = anthropic.Anthropic()

    existing_ids = load_existing_video_ids(bq)
    all_transcripts = load_transcripts(bq)
    to_process = [t for t in all_transcripts if t["video_id"] not in existing_ids]

    if not to_process:
        context.log.info("Nothing to process — all transcripts already extracted.")
        return dg.MaterializeResult(metadata={"new_predictions": 0})

    context.log.info(f"Processing {len(to_process)} video(s)")

    total_new = 0
    for i, item in enumerate(to_process, 1):
        vid_id = item["video_id"]
        context.log.info(f"[{i}/{len(to_process)}] {vid_id}")

        raw_preds = extract_predictions_from_transcript(client, vid_id, item["transcript_text"])
        rows = build_prediction_rows(vid_id, item["video_url"], raw_preds, publish_date=item["publish_date"])

        if rows:
            insert_to_bq(bq, rows)
            total_new += len(rows)
            context.log.info(f"  {len(rows)} prediction(s) inserted")

    return dg.MaterializeResult(metadata={"new_predictions": total_new})


@dg.asset(
    key="manual_match_results",
    deps=["raw_predictions_extracted"],
    description="Manually ingest match results from data/inbox/results*.csv → raw.match_results (fallback for TheSportsDB misses)",
    group_name="ingestion",
    kinds={"python", "bigquery"},
)
def ingest_manual_results(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    from google.cloud import bigquery

    from src.results.ingest_results import (
        load_existing_result_ids,
        process_file,
    )

    bq = bigquery.Client(project="maldinia")
    existing_result_ids = load_existing_result_ids(bq)

    csv_files = sorted(INBOX.glob("results*.csv"))
    if not csv_files:
        context.log.info("No results*.csv files found in data/inbox/ — nothing to ingest.")
        return dg.MaterializeResult(metadata={"new_results": 0})

    total = 0
    for path in csv_files:
        context.log.info(f"Processing {path.name}")
        n = process_file(bq, path, existing_result_ids, dry_run=False)
        total += n
        context.log.info(f"  {n} result(s) inserted from {path.name}")

    return dg.MaterializeResult(metadata={"new_results": total})


@dg.asset(
    key="raw_match_results",
    deps=["raw_predictions_extracted"],
    description="Fetch match results from TheSportsDB for pending predictions → raw.match_results",
    group_name="ingestion",
    kinds={"python", "bigquery"},
)
def fetch_results(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    from google.cloud import bigquery

    from src.results.fetch_results import (
        load_pending_predictions,
        infer_season,
        fetch_matches_for_competition,
        match_prediction_to_result,
        match_aggregate_to_result,
        insert_results_to_bq,
        COMPETITION_MAP,
    )

    bq = bigquery.Client(project="maldinia")

    pending = load_pending_predictions(bq, competition_filter=None)
    context.log.info(f"{len(pending)} pending prediction(s)")

    if not pending:
        return dg.MaterializeResult(metadata={"matched": 0, "not_found": 0})

    # Build fetch queue
    seen_comp_seasons: set[tuple[str, str]] = set()
    fetch_queue: list[tuple[str, str]] = []

    for r in pending:
        comp = r["competition"]
        ref_date = str(r.get("match_date") or r.get("publish_date") or "")
        season = infer_season(comp, ref_date)
        key = (comp, season)
        if key not in seen_comp_seasons:
            seen_comp_seasons.add(key)
            fetch_queue.append(key)

    # Pre-fetch API matches
    api_matches_cache: dict[str, list[dict]] = {}
    for comp, season in fetch_queue:
        league_id = COMPETITION_MAP.get(comp)
        if league_id is None:
            context.log.warning(f"No league ID for competition: {comp}")
            continue

        context.log.info(f"Fetching {comp} (season {season})…")
        matches = fetch_matches_for_competition(league_id, season, comp)
        api_matches_cache.setdefault(comp, []).extend(matches)
        context.log.info(f"  {len(matches)} finished match(es)")

    # Match predictions to results
    result_rows = []
    not_found = 0

    for pred in pending:
        comp = pred["competition"]
        api_matches = api_matches_cache.get(comp, [])
        is_aggregate = str(pred.get("match_type", "single")) == "aggregate"

        if is_aggregate:
            result = match_aggregate_to_result(pred, api_matches)
        else:
            result = match_prediction_to_result(pred, api_matches)

        if result is None:
            not_found += 1
            continue

        result_rows.append({
            "result_id":     pred["prediction_id"] + "_result",
            "prediction_id": pred["prediction_id"],
            "competition":   comp,
            "home_team":     pred["home_team"],
            "away_team":     pred["away_team"],
            "home_goals":    result["actual_home_goals"],
            "away_goals":    result["actual_away_goals"],
            "actual_result": result["actual_result"],
            "match_date":    result.get("match_date", ""),
        })

    context.log.info(f"Matched: {len(result_rows)}  |  not found: {not_found}")

    if result_rows:
        insert_results_to_bq(bq, result_rows)

    return dg.MaterializeResult(
        metadata={"matched": len(result_rows), "not_found": not_found}
    )
