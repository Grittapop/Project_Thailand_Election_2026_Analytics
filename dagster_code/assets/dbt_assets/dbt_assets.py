from dagster_dbt import dbt_assets, DbtCliResource

@dbt_assets(
    manifest="/opt/dagster/app/dbt_election/target/manifest.json",
)
def dbt_gold_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(
        ["build", "--select", "gold"],
        context=context,
    ).stream()