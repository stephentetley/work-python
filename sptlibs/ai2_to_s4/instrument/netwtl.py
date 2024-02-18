"""
Copyright 2024 Stephen Tetley

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import duckdb
import polars as pl



eav_sample = pl.DataFrame([ 
    {'sai_num': 'X001', 'attr_name': 'Location On Site', 'attr_value': 'Kiosk'},
])


def extract_chars(*, df: pl.DataFrame) -> pl.DataFrame: 
    return df.select(
        [ (pl.col("sai_num"))
        , (pl.col("Location On Site").alias("location_on_site"))
        ]
    )



netwtl_ddl = """
    CREATE OR REPLACE TABLE ai2_to_s4.netwtl  (
        equi VARCHAR,
        uniclass_code VARCHAR,
        uniclass_desc VARCHAR,
        location_on_site VARCHAR,
        memo_line VARCHAR,
        manufacturers_asset_life_yr INTEGER,
        ip_rating VARCHAR,
        netw_supply_voltage INTEGER,
        netw_supply_voltage_units VARCHAR,
        PRIMARY KEY(equi)
    );
    """


def netwtl_insert(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_to_s4.netwtl BY NAME
    SELECT 
        df.sai_num AS equi,
        df.location_on_site AS location_on_site,
    FROM {df_view_name} AS df
    WHERE df.sai_num <> 'X001'
    ON CONFLICT DO NOTHING;
    """

def store_class(*, con: duckdb.DuckDBPyConnection, exec_ddl: bool, df: pl.DataFrame) -> None:
    con.register(view_name='vw_netwtl_df', python_object=df)
    if exec_ddl:
        con.execute(netwtl_ddl)
    con.execute(netwtl_insert(df_view_name="vw_netwtl_df"))
