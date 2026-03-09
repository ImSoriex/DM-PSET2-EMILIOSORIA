from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres

import pandas as pd
from datetime import datetime, timezone
from os import path as ospath

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


@data_exporter
def load_local_to_bronze_trips(manifest, *args, **kwargs):
    assert isinstance(manifest, list)

    config_path = ospath.join(get_repo_path(), "io_config.yaml")
    config_profile = "default"

    files = [x for x in manifest if x.get("status") in ("exists", "downloaded")]
    print(f"Files to load: {len(files)}")

    loaded = 0

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        for item in files:
            st = item["service_type"]
            ym = item["source_month"]
            file_path = item["path"]

            table_name = "yellow_trips" if st == "yellow" else "green_trips"

            df = pd.read_parquet(file_path)
            df = normalize_cols(df)
            df["ingest_ts"] = datetime.now(timezone.utc)
            df["source_month"] = ym
            df["service_type"] = st

            safe_ym = ym.replace("'", "''")

            # 1) si tabla existe, borramos el mes; si no existe, saltamos delete
            try:
                if loader.table_exists("bronze", table_name):
                    loader.execute(f"DELETE FROM bronze.{table_name} WHERE source_month = '{safe_ym}';")
            except Exception as e:
                # limpiar estado de transacción
                try:
                    loader.conn.rollback()
                except Exception:
                    pass
                print(f"DELETE failed (will continue) bronze.{table_name} {ym}: {e}")

            # 2) export: append si existe, si no existe -> replace
            try:
                if loader.table_exists("bronze", table_name):
                    loader.export(
                        df,
                        "bronze",
                        table_name,
                        index=False,
                        if_exists="append",
                        chunksize=500_000,
                    )
                else:
                    loader.export(
                        df,
                        "bronze",
                        table_name,
                        index=False,
                        if_exists="replace",
                        chunksize=500_000,
                    )
            except Exception as e:
                try:
                    loader.conn.rollback()
                except Exception:
                    pass
                # fallback final
                loader.export(
                    df,
                    "bronze",
                    table_name,
                    index=False,
                    if_exists="replace",
                    chunksize=500_000,
                )

            loaded += 1
            print(f"Loaded bronze.{table_name} {ym}: {len(df):,} rows")

    return {"status": "ok", "files_loaded": loaded}


@test
def test_output(output, *args):
    assert output is not None
    assert output.get("status") == "ok"