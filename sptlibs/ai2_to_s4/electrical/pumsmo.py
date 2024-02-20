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



eav_sample = pl.DataFrame(
    [ {'sai_num': 'X001', 'attr_name': 'Location On Site', 'attr_value': 'Wet Well'}
    , {'sai_num': 'X001', 'attr_name': 'Speed (RPM)', 'attr_value': '100'}
    , {'sai_num': 'X001', 'attr_name': 'Impeller Type', 'attr_value': 'N Adaptive'}
    ]
)


def extract_chars(*, df: pl.DataFrame) -> pl.DataFrame: 
    return df.select(
        [ (pl.col("sai_num"))
        , (pl.col("Location On Site").alias("location_on_site"))
        , (pl.col("Speed (RPM)").cast(pl.Int32).alias("speed_rpm"))
        , (pl.col("Impeller Type").str.to_uppercase().alias("impeller_type"))
        ]
    )


    

pumsmo_ddl = """
    CREATE OR REPLACE TABLE ai2_to_s4.pumsmo  (
        equi VARCHAR,
        uniclass_code VARCHAR,
        uniclass_desc VARCHAR,
        location_on_site VARCHAR,
        memo_line VARCHAR,
        manufacturers_asset_life_yr INTEGER,
        pums_flow_litres_per_sec DECIMAL(18, 3),
        pums_impeller_type VARCHAR,
        pums_inlet_size_mm INTEGER,
        pums_installed_design_head_m DECIMAL(18, 3),
        insulation_class_deg_c VARCHAR,
        ip_rating VARCHAR,
        pums_lifting_type VARCHAR,
        pums_media_type VARCHAR,
        pums_outlet_size_mm INTEGER,
        pums_rated_current_a DECIMAL(18, 3),
        pums_rated_power_kw DECIMAL(18, 3),
        pums_rated_speed_rpm INTEGER,
        pums_rated_voltage INTEGER,
        pums_rated_voltage_units VARCHAR,
        PRIMARY KEY(equi)
    );
    """


def pumsmo_insert(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_to_s4.pumsmo BY NAME
    SELECT 
        df.sai_num AS equi,
        df.location_on_site AS location_on_site,
        df.speed_rpm AS pums_rated_speed_rpm,
        df.impeller_type AS pums_impeller_type,
    FROM {df_view_name} AS df
    WHERE df.sai_num <> 'X001'
    ON CONFLICT DO NOTHING;
    """

def store_class(*, con: duckdb.DuckDBPyConnection, exec_ddl: bool, df: pl.DataFrame) -> None:
    con.register(view_name='vw_pumsmo_df', python_object=df)
    if exec_ddl:
        con.execute(pumsmo_ddl)
    con.execute(pumsmo_insert(df_view_name="vw_pumsmo_df"))
