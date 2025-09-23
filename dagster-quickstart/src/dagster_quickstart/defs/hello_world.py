import dagster as dg


@dg.asset
def hello(context: dg.AssetExecutionContext):
    context.log.info("Hello!")
    return "Hello"


@dg.asset(deps=[hello])
def world(context: dg.AssetExecutionContext):
    context.log.info("World!")
    return "World"
