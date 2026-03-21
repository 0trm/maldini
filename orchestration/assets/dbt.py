"""
Dagster assets from dbt models.

Each dbt model automatically becomes a Dagster asset with the correct
dependency graph. The dagster-dbt integration parses dbt's manifest.json
to discover models and their dependencies.
"""

from pathlib import Path

from dagster_dbt import DbtProject, DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "maldini_dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR  # profiles.yml lives inside maldini_dbt/

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_maldini_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["run"], context=context).stream()
