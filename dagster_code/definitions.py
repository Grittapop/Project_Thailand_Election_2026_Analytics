from dagster import Definitions, load_assets_from_package_module
import dagster_code.assets as assets
from dagster_code.resources import s3, trino_resource
#from dagster_code.jobs import full_pipeline_job
#from dagster_code.schedules import daily_pipeline

all_assets = load_assets_from_package_module(assets)

defs = Definitions(
    assets=all_assets,
    #jobs=[full_pipeline_job],
    #schedules=[daily_pipeline],
    resources={
        "s3": s3,
        "trino": trino_resource,
    },
)