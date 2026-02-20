from dagster import Definitions, load_assets_from_package_module, define_asset_job, AssetSelection, ScheduleDefinition
import dagster_code.assets as assets
from dagster_code.resources import s3, trino_resource, dbt


all_assets = load_assets_from_package_module(assets)


full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.groups("silver") | AssetSelection.groups("default"),
)


daily_full_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 2 * * *",  
)


defs = Definitions(
    assets=all_assets,
    jobs=[full_pipeline_job],
    schedules=[daily_full_schedule],
    resources={
        "s3": s3,
        "trino": trino_resource,
        "dbt": dbt,
    },
)