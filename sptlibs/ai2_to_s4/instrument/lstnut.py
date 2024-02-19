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
    , {'sai_num': 'X001', 'attr_name': 'Transducer Type', 'attr_value': 'XPS-15'}
    , {'sai_num': 'X001', 'attr_name': 'Transducer Serial No', 'attr_value': 'LDN01'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 1 Function', 'attr_value': 'UNDESIGNATED LEVEL ALARM'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 1 on Level (m)', 'attr_value': '1.95'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 1 off Level (m)', 'attr_value': '1.90'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 2 Function', 'attr_value': 'UNDESIGNATED LEVEL ALARM'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 2 on Level (m)', 'attr_value': '0.95'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 2 off Level (m)', 'attr_value': '0.90'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 3 Function', 'attr_value': 'UNDESIGNATED LEVEL ALARM'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 3 on Level (m)', 'attr_value': '0.95'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 3 off Level (m)', 'attr_value': '0.90'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 4 Function', 'attr_value': 'UNDESIGNATED LEVEL ALARM'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 4 on Level (m)', 'attr_value': '0.95'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 4 off Level (m)', 'attr_value': '0.90'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 5 Function', 'attr_value': 'UNDESIGNATED LEVEL ALARM'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 5 on Level (m)', 'attr_value': '0.95'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 5 off Level (m)', 'attr_value': '0.90'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 6 Function', 'attr_value': 'UNDESIGNATED LEVEL ALARM'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 6 on Level (m)', 'attr_value': '0.95'}
    , {'sai_num': 'X001', 'attr_name': 'Relay 6 off Level (m)', 'attr_value': '0.90'}
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
        , (pl.col("Transducer Type").alias("transducer_type"))
        , (pl.col("Transducer Serial No").alias("transducer_serial_no"))
        , (pl.col("Relay 1 Function").alias("relay_1_function"))
        , (pl.col("Relay 1 on Level (m)").cast(pl.Float64).alias("relay_1_on_level"))
        , (pl.col("Relay 1 off Level (m)").cast(pl.Float64).alias("relay_1_off_level"))
        , (pl.col("Relay 2 Function").alias("relay_2_function"))
        , (pl.col("Relay 2 on Level (m)").cast(pl.Float64).alias("relay_2_on_level"))
        , (pl.col("Relay 2 off Level (m)").cast(pl.Float64).alias("relay_2_off_level"))
        , (pl.col("Relay 3 Function").alias("relay_3_function"))
        , (pl.col("Relay 3 on Level (m)").cast(pl.Float64).alias("relay_3_on_level"))
        , (pl.col("Relay 3 off Level (m)").cast(pl.Float64).alias("relay_3_off_level"))
        , (pl.col("Relay 4 Function").alias("relay_4_function"))
        , (pl.col("Relay 4 on Level (m)").cast(pl.Float64).alias("relay_4_on_level"))
        , (pl.col("Relay 4 off Level (m)").cast(pl.Float64).alias("relay_4_off_level"))
        , (pl.col("Relay 5 Function").alias("relay_5_function"))
        , (pl.col("Relay 5 on Level (m)").cast(pl.Float64).alias("relay_5_on_level"))
        , (pl.col("Relay 5 off Level (m)").cast(pl.Float64).alias("relay_5_off_level"))
        , (pl.col("Relay 6 Function").alias("relay_6_function"))
        , (pl.col("Relay 6 on Level (m)").cast(pl.Float64).alias("relay_6_on_level"))
        , (pl.col("Relay 6 off Level (m)").cast(pl.Float64).alias("relay_6_off_level"))
        , (pl.format("{} - {} {}", pl.col("Signal min"), pl.col("Signal max"), pl.col("Signal unit").str.to_uppercase()).alias("signal"))
        , (pl.col("Range min").cast(pl.Float64).alias("range_min"))
        , (pl.col("Range max").cast(pl.Float64).alias("range_max"))
        , (pl.col("Range unit").str.to_uppercase().alias("range_units"))
        ]
    )


    

lstnut_ddl = """
    CREATE OR REPLACE TABLE ai2_to_s4.lstnut (
        equi VARCHAR,
        uniclass_code VARCHAR,
        uniclass_desc VARCHAR,
        location_on_site VARCHAR,
        memo_line VARCHAR,
        manufacturers_asset_life_yr INTEGER,
        ip_rating VARCHAR,
        lstn_signal_type VARCHAR,
        lstn_range_max DECIMAL(18, 3),
        lstn_range_min DECIMAL(18, 3),
        lstn_range_units VARCHAR,
        lstn_relay_1_function VARCHAR,
        lstn_relay_1_on_level_m DECIMAL(18, 3),
        lstn_relay_1_off_level_m DECIMAL(18, 3),
        lstn_relay_2_function VARCHAR,
        lstn_relay_2_on_level_m DECIMAL(18, 3),
        lstn_relay_2_off_level_m DECIMAL(18, 3),
        lstn_relay_3_function VARCHAR,
        lstn_relay_3_on_level_m DECIMAL(18, 3),
        lstn_relay_3_off_level_m DECIMAL(18, 3),
        lstn_relay_4_function VARCHAR,
        lstn_relay_4_on_level_m DECIMAL(18, 3),
        lstn_relay_4_off_level_m DECIMAL(18, 3),
        lstn_relay_5_function VARCHAR,
        lstn_relay_5_on_level_m DECIMAL(18, 3),
        lstn_relay_5_off_level_m DECIMAL(18, 3),
        lstn_relay_6_function VARCHAR,
        lstn_relay_6_on_level_m DECIMAL(18, 3),
        lstn_relay_6_off_level_m DECIMAL(18, 3),
        lstn_set_to_snort VARCHAR,
        lstn_supply_voltage INTEGER,
        lstn_supply_voltage_units VARCHAR,
        lstn_transducer_model VARCHAR,
        lstn_transducer_serial_no VARCHAR,
        lstn_transmitter_model VARCHAR,
        lstn_transmitter_serial_no VARCHAR,
        PRIMARY KEY(equi)
    );
    """


def lstnut_insert(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_to_s4.lstnut BY NAME
    SELECT 
        df.sai_num AS equi,
        df.location_on_site AS location_on_site,
        df.transducer_type AS lstn_transducer_model,
        df.transducer_serial_no AS lstn_transducer_serial_no,
        df.relay_1_function AS lstn_relay_1_function,
        df.relay_1_on_level AS lstn_relay_1_on_level_m,
        df.relay_1_off_level AS lstn_relay_1_off_level_m,
        df.relay_2_function AS lstn_relay_2_function,
        df.relay_2_on_level AS lstn_relay_2_on_level_m,
        df.relay_2_off_level AS lstn_relay_2_off_level_m,
        df.relay_3_function AS lstn_relay_3_function,
        df.relay_3_on_level AS lstn_relay_3_on_level_m,
        df.relay_3_off_level AS lstn_relay_3_off_level_m,
        df.relay_4_function AS lstn_relay_4_function,
        df.relay_4_on_level AS lstn_relay_4_on_level_m,
        df.relay_4_off_level AS lstn_relay_4_off_level_m,
        df.relay_5_function AS lstn_relay_5_function,
        df.relay_5_on_level AS lstn_relay_5_on_level_m,
        df.relay_5_off_level AS lstn_relay_5_off_level_m,
        df.relay_6_function AS lstn_relay_6_function,
        df.relay_6_on_level AS lstn_relay_6_on_level_m,
        df.relay_6_off_level AS lstn_relay_6_off_level_m,
        df.signal AS lstn_signal_type,
        df.range_min AS lstn_range_min,
        df.range_max AS lstn_range_max,
        df.range_units AS lstn_range_units,
    FROM {df_view_name} AS df
    WHERE df.sai_num <> 'X001'
    ON CONFLICT DO NOTHING;
    """

def store_class(*, con: duckdb.DuckDBPyConnection, exec_ddl: bool, df: pl.DataFrame) -> None:
    con.register(view_name='vw_lstnut_df', python_object=df)
    if exec_ddl:
        con.execute(lstnut_ddl)
    con.execute(lstnut_insert(df_view_name="vw_lstnut_df"))
