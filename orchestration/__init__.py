import dagster as dg
from dagster_dbt import DbtCliResource

from orchestration.assets.ingestion import (
    ingest_transcripts,
    extract_predictions,
    fetch_results,
    ingest_manual_results,
)
from orchestration.assets.dbt import dbt_maldini_assets, DBT_PROJECT_DIR, DBT_PROFILES_DIR
from orchestration.sensors import inbox_sensor, results_sensor

all_assets = [
    ingest_transcripts,
    extract_predictions,
    fetch_results,
    ingest_manual_results,
    dbt_maldini_assets,
]

# Sensor-triggered: full pipeline from CSV drop to dbt
full_pipeline_job = dg.define_asset_job(
    name="full_pipeline",
    selection=dg.AssetSelection.all() - dg.AssetSelection.assets("manual_match_results"),
)

# Scheduled daily: skips ingest, re-scores predictions from newly played matches
daily_job = dg.define_asset_job(
    name="daily_pipeline",
    selection=(
        dg.AssetSelection.all()
        - dg.AssetSelection.assets("raw_transcripts")
        - dg.AssetSelection.assets("manual_match_results")
    ),
)

# Manual fallback: triggered by results_sensor when results*.csv appears
manual_results_job = dg.define_asset_job(
    name="manual_results_job",
    selection=dg.AssetSelection.assets("manual_match_results"),
)

daily_schedule = dg.ScheduleDefinition(
    name="daily_schedule",
    job=daily_job,
    cron_schedule="0 8 * * *",  # 08:00 UTC daily
)

defs = dg.Definitions(
    assets=all_assets,
    jobs=[full_pipeline_job, daily_job, manual_results_job],
    schedules=[daily_schedule],
    sensors=[inbox_sensor, results_sensor],
    resources={
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
        ),
    },
)
