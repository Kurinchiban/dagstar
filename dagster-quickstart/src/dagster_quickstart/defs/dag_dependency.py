from typing import List
import dagster as dg

@dg.asset(group_name="grouping_assets_test")
def my_first_asset(context: dg.AssetExecutionContext):
    data = [1, 2, 3, 4]
    context.log.info(f"Output of my_first_asset: {data}")
    return data

@dg.asset(ins={"upstream": dg.AssetIn(key="my_first_asset")},group_name="grouping_assets_test")
def my_second_asset(context: dg.AssetExecutionContext, upstream: List):
    data = upstream + [5, 6, 7, 8, 9]
    context.log.info(f"Output of my_second_asset: {data}")
    return data

# Defines the inputs (dependencies) for this asset.
@dg.asset(
    ins={
        "first_upstream": dg.AssetIn(key="my_first_asset"),
        "second_upstream": dg.AssetIn(key="my_second_asset"),
    },group_name="grouping_assets_test"
)
def my_third_asset(context: dg.AssetExecutionContext, first_upstream: List, second_upstream: List):
    data = first_upstream + second_upstream
    context.log.info(f"Output of my_third_asset: {data}")
    return data

defs = dg.Definitions(
    assets=[my_first_asset, my_second_asset, my_third_asset],
    jobs=[
        dg.define_asset_job(
            name="dagster_dependency",
            selection=dg.AssetSelection.groups("grouping_assets_test")
        )
    ],
    schedules=[
        dg.ScheduleDefinition(
            name = "dagster_dependency_schedule",
            job_name = "dagster_dependency",
            cron_schedule="*/5 * * * *"
        )
    ]
)