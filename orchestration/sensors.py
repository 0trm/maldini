"""
Dagster sensors for the Maldini Stats pipeline.
"""

from pathlib import Path

import dagster as dg

ROOT = Path(__file__).resolve().parents[1]
INBOX = ROOT / "data" / "inbox"


@dg.sensor(
    job_name="full_pipeline",
    minimum_interval_seconds=60,
    description="Triggers full_pipeline when a video CSV appears in data/inbox/ (excludes results*.csv)",
)
def inbox_sensor(context: dg.SensorEvaluationContext):
    csv_files = [
        f for f in sorted(INBOX.glob("*.csv"))
        if not f.name.startswith("results")
    ]
    if not csv_files:
        return dg.SkipReason("No video CSV files in data/inbox/")

    file_names = [f.name for f in csv_files]
    context.log.info(f"Detected inbox files: {file_names}")
    yield dg.RunRequest(
        run_key=",".join(file_names),
        run_config={},
        tags={"triggered_by": "inbox_sensor", "inbox_files": str(file_names)},
    )


@dg.sensor(
    job_name="manual_results_job",
    minimum_interval_seconds=60,
    description="Triggers manual_results_job when a results*.csv appears in data/inbox/",
)
def results_sensor(context: dg.SensorEvaluationContext):
    csv_files = sorted(INBOX.glob("results*.csv"))
    if not csv_files:
        return dg.SkipReason("No results*.csv files in data/inbox/")

    file_names = [f.name for f in csv_files]
    context.log.info(f"Detected results files: {file_names}")
    yield dg.RunRequest(
        run_key=",".join(file_names),
        run_config={},
        tags={"triggered_by": "results_sensor", "inbox_files": str(file_names)},
    )
