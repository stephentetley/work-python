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
    [ {'sai_num': 'X001', 'attr_name': 'Location On Site', 'attr_value': 'Outfalll'}
    , {'sai_num': 'X001', 'attr_name': 'Pipe Diameter mm', 'attr_value': '100'}
    , {'sai_num': 'X001', 'attr_name': 'Signal min', 'attr_value': '0.0'}
    , {'sai_num': 'X001', 'attr_name': 'Signal max', 'attr_value': '0.0'}
    , {'sai_num': 'X001', 'attr_name': 'Signal unit', 'attr_value': 'l/s'}
    , {'sai_num': 'X001', 'attr_name': 'Range min', 'attr_value': '0.0'}
    , {'sai_num': 'X001', 'attr_name': 'Range max', 'attr_value': '0.0'}
    , {'sai_num': 'X001', 'attr_name': 'Range unit', 'attr_value': 'mA'}
    ]
)


def extract_chars(*, df: pl.DataFrame) -> pl.DataFrame: 
    return df.select(
        [ (pl.col("sai_num"))
        , (pl.col("Location On Site").alias("location_on_site"))
        , (pl.col("Pipe Diameter mm").cast(pl.Int32).alias("pipe_diameter_mm"))
        , (pl.format("{} - {} {}", pl.col("Signal min"), pl.col("Signal max"), pl.col("Signal unit").str.to_uppercase()).alias("signal"))
        , (pl.col("Range min").cast(pl.Decimal).alias("range_min"))
        , (pl.col("Range max").cast(pl.Decimal).alias("range_max"))
        , (pl.col("Range unit").str.to_uppercase().alias("range_units"))
        ]
    )


    

fstnem_ddl = """
    CREATE OR REPLACE TABLE ai2_to_s4.fstnem  (
        equi VARCHAR,
        uniclass_code VARCHAR,
        uniclass_desc VARCHAR,
        location_on_site VARCHAR,
        memo_line VARCHAR,
        manufacturers_asset_life_yr INTEGER,
        fstn_field_validation_device VARCHAR,
        ip_rating VARCHAR,
        fstn_signal_type VARCHAR,
        fstn_pipe_diameter_mm INTEGER,
        fstn_range_max DECIMAL(18, 3),
        fstn_range_min DECIMAL(18, 3),
        fstn_range_units VARCHAR,
        fstn_sens_calibration_factor_1 DECIMAL(18, 3),
        fstn_sens_calibration_factor_2 DECIMAL(18, 3),
        fstn_sens_calibration_factor_3 DECIMAL(18, 3),
        fstn_sens_calibration_factor_4 DECIMAL(18, 3),
        fstn_transducer_model VARCHAR,
        fstn_transducer_serial_no VARCHAR,
        fstn_transmitter_model VARCHAR,
        fstn_transmitter_serial_no VARCHAR,
        fstn_rated_voltage INTEGER,
        fstn_rated_voltage_units VARCHAR,
        PRIMARY KEY(equi)
    );
    """


def fstnem_insert(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_to_s4.fstnem BY NAME
    SELECT 
        df.sai_num AS equi,
        df.location_on_site AS location_on_site,
        df.pipe_diameter_mm AS fstn_pipe_diameter_mm,
        df.signal AS fstn_signal_type,
        df.range_min AS fstn_range_min,
        df.range_max AS fstn_range_max,
        df.range_units AS fstn_range_units,
    FROM {df_view_name} AS df
    WHERE df.sai_num <> 'X001'
    ON CONFLICT DO NOTHING;
    """

def store_class(*, con: duckdb.DuckDBPyConnection, exec_ddl: bool, df: pl.DataFrame) -> None:
    con.register(view_name='vw_fstnem_df', python_object=df)
    if exec_ddl:
        con.execute(fstnem_ddl)
    con.execute(fstnem_insert(df_view_name="vw_fstnem_df"))
