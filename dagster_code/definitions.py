from dagster import Definitions, load_assets_from_package_module
import dagster_code.assets as assets
from dagster_code.resources import s3

all_assets = load_assets_from_package_module(assets)

defs = Definitions(
    assets=all_assets,
    resources={
        "s3": s3,
    },
)


