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


pivot_columns = [ 
    "location_on_site",
    "range_max",
    "range_min", 
    "range_unit",
    "relay_1_function", 
    "relay_1_on_level_m", 
    "relay_1_off_level_m",
    "relay_2_function",
    "relay_2_on_level_m",
    "relay_2_off_level_m",
    "relay_3_function",
    "relay_3_on_level_m",
    "relay_3_off_level_m",
    "relay_4_function",
    "relay_4_on_level_m",
    "relay_4_off_level_m",
    "relay_5_function",
    "relay_5_on_level_m",
    "relay_5_off_level_m",
    "relay_6_function",
    "relay_6_on_level_m",
    "relay_6_off_level_m",
    "signal_min",
    "signal_max",
    "signal_unit",
    "transducer_serial_no",
    "transducer_type"
    ]


def extract_chars(df: pl.DataFrame) -> pl.DataFrame: 
    return df.select([ 
        (pl.col("ai2_reference").alias("equi_id")),
        (pl.lit("").alias("uniclass_code")),
        (pl.lit("").alias("uniclass_desc")),
        (pl.col("location_on_site").alias("location_on_site")),
        (pl.col("range_min").str.to_decimal().alias("lstn_range_min")),
        (pl.col("range_max").str.to_decimal().alias("lstn_range_max")),
        (pl.col("range_unit").str.to_uppercase().alias("lstn_range_units")),
        (pl.col("relay_1_function").alias("lstn_relay_1_function")),
        (pl.col("relay_1_on_level_m").str.to_decimal().alias("lstn_relay_1_on_level_m")),
        (pl.col("relay_1_off_level_m").str.to_decimal().alias("lstn_relay_1_off_level_m")),
        (pl.col("relay_2_function").alias("lstn_relay_2_function")),
        (pl.col("relay_2_on_level_m").str.to_decimal().alias("lstn_relay_2_on_level_m")),
        (pl.col("relay_2_off_level_m").str.to_decimal().alias("lstn_relay_2_off_level_m")),
        (pl.col("relay_3_function").alias("lstn_relay_3_function")),
        (pl.col("relay_3_on_level_m").str.to_decimal().alias("lstn_relay_3_on_level_m")),
        (pl.col("relay_3_off_level_m").str.to_decimal().alias("lstn_relay_3_off_level_m")),
        (pl.col("relay_4_function").alias("lstn_relay_4_function")),
        (pl.col("relay_4_on_level_m").str.to_decimal().alias("lstn_relay_4_on_level_m")),
        (pl.col("relay_4_off_level_m").str.to_decimal().alias("lstn_relay_4_off_level_m")),
        (pl.col("relay_5_function").alias("lstn_relay_5_function")),
        (pl.col("relay_5_on_level_m").str.to_decimal().alias("lstn_relay_5_on_level_m")),
        (pl.col("relay_5_off_level_m").str.to_decimal().alias("lstn_relay_5_off_level_m")),
        (pl.col("relay_6_function").alias("lstn_relay_6_function")),
        (pl.col("relay_6_on_level_m").str.to_decimal().alias("lstn_relay_6_on_level_m")),
        (pl.col("relay_6_off_level_m").str.to_decimal().alias("lstn_relay_6_off_level_m")),
        (pl.format("{} - {} {}", pl.col("signal_min"), pl.col("signal_max"), pl.col("signal_unit").str.to_uppercase()).alias("output_signal_type")),
        (pl.col("transducer_serial_no").alias("lstn_transducer_serial_no")),
        (pl.col("transducer_type").alias("lstn_transducer_model")),
        ])




def lstnut_insert_stmt(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_class_rep.lstnut BY NAME
    SELECT 
        df.equi_id AS equi_id,
        df.location_on_site AS location_on_site,
        df.range_min AS lstn_range_min,
        df.range_max AS lstn_range_max,
        df.range_units AS lstn_range_units,
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
        df.transducer_type AS lstn_transducer_model,
        df.transducer_serial_no AS lstn_transducer_serial_no,
    FROM {df_view_name} AS df
    ON CONFLICT DO NOTHING;
    """
