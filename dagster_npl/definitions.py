from dagster import Definitions, load_assets_from_modules

from dagster_npl import assets, npl_main_load

all_assets = load_assets_from_modules([assets])


defs = Definitions(
    assets=all_assets,
    jobs=[npl_main_load.npl_job,],
)
