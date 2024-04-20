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
import sptlibs.asset_ir.ai2_class_rep.utils as utils
import sptlibs.asset_ir.class_rep.gen_table as gen_table

def create_fstnem_table(*, con: duckdb.DuckDBPyConnection) -> None: 
    gen_table.gen_cr_table(pk_name='equi_id', schema_name='ai2_class_rep', class_name='FSTNEM', con=con)


def ingest_fstnem_eav_data(*, con: duckdb.DuckDBPyConnection) -> None: 
    utils.ingest_equipment_eav_data(
        equipment_ai2_name='ULTRASONIC LEVEL INSTRUMENT', 
        equipment_ai2_column_names=pivot_columns, 
        extract_trafo=extract_fstnem_chars, 
        insert_stmt=fstnem_insert_stmt,
        df_view_name='df_fstnem_vw',
        con=con)


pivot_columns = [ 
    "location_on_site",
    "pipe_diameter_mm",
    "range_max",
    "range_min", 
    "range_unit",
    "sensor_calibration_factor_1",
    "sensor_calibration_factor_2",
    "sensor_calibration_factor_3",
    "sensor_calibration_factor_4",
    "signal_min",
    "signal_max",
    "signal_unit",
    ]


def extract_fstnem_chars(df: pl.DataFrame) -> pl.DataFrame: 
    return df.select([ 
        (pl.col("ai2_reference").alias("equi_id")),
        (pl.lit("").alias("uniclass_code")),
        (pl.lit("").alias("uniclass_desc")),
        (pl.col("location_on_site").alias("location_on_site")),
        (pl.col("pipe_diameter_mm").cast(pl.Int64, strict=False).alias("fstn_pipe_diameter_mm")),
        (pl.col("range_min").cast(pl.Float64, strict=False).alias("fstn_range_min")),
        (pl.col("range_max").cast(pl.Float64, strict=False).alias("fstn_range_max")),
        (pl.col("range_unit").str.to_uppercase().alias("fstn_range_units")),
        (pl.col("sensor_calibration_factor_1").cast(pl.Float64, strict=False).alias("fstn_sens_calibration_factor_1")),
        (pl.col("sensor_calibration_factor_2").cast(pl.Float64, strict=False).alias("fstn_sens_calibration_factor_2")),
        (pl.col("sensor_calibration_factor_3").cast(pl.Float64, strict=False).alias("fstn_sens_calibration_factor_3")),
        (pl.col("sensor_calibration_factor_4").cast(pl.Float64, strict=False).alias("fstn_sens_calibration_factor_4")),
        (pl.format("{} - {} {}", pl.col("signal_min"), pl.col("signal_max"), pl.col("signal_unit").str.to_uppercase()).alias("fstn_signal_type")),
        ])




fstnem_insert_stmt = """
    INSERT INTO ai2_class_rep.equi_fstnem BY NAME
    SELECT 
        df.equi_id AS equi_id,
        df.location_on_site AS location_on_site,
        df.fstn_pipe_diameter_mm AS fstn_pipe_diameter_mm,
        df.fstn_range_min AS fstn_range_min,
        df.fstn_range_max AS fstn_range_max,
        df.fstn_range_units AS fstn_range_units,
        df.fstn_signal_type AS fstn_signal_type,
        df.fstn_sens_calibration_factor_1 AS fstn_sens_calibration_factor_1,
        df.fstn_sens_calibration_factor_2 AS fstn_sens_calibration_factor_2,
        df.fstn_sens_calibration_factor_3 AS fstn_sens_calibration_factor_3,
        df.fstn_sens_calibration_factor_4 AS fstn_sens_calibration_factor_4,
    FROM df_fstnem_vw AS df
    ON CONFLICT DO NOTHING;
    """
