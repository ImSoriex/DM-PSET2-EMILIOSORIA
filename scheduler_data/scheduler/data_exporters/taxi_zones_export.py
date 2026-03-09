import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path as ospath

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def taxi_zones_export(data, *args, **kwargs):

    file_path = data["path"]

    df = pd.read_csv(file_path)

    df.columns = [c.strip().lower() for c in df.columns]

    config_path = ospath.join(get_repo_path(), "io_config.yaml")
    config_profile = "default"

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:

        loader.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        loader.export(
            df,
            "bronze",
            "taxi_zone_lookup",
            index=False,
            if_exists="replace"
        )

    print(f"Loaded {len(df)} rows into bronze.taxi_zone_lookup")

    return {"status": "ok"}