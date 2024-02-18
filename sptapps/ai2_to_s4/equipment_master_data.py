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



# NOTE
# Best way to insure the pivot table has necessary columns is to add fake recods with them.
# sai_num     ┆ attr_name            ┆ attr_value
# Doing this then pivoting solves the ai2 to s4 translation puzzle

eav_sample = pl.DataFrame([ 
    {'sai_num': 'X001', 'attr_name': 'Common Name', 'attr_value': 'ABC/XY/OUTSTATION-1/EQUIPMENT: TELEMETRY OUTSTATION'},
    {'sai_num': 'X001', 'attr_name': 'Installed From', 'attr_value': '06/30/23 12:00'},
    {'sai_num': 'X001', 'attr_name': 'Manufacturer', 'attr_value': 'LESTER CORP'},
    {'sai_num': 'X001', 'attr_name': 'Model', 'attr_value': 'MACH 1'},
    {'sai_num': 'X001', 'attr_name': 'AssetStatus', 'attr_value': 'OPERATIONAL'},
    {'sai_num': 'X001', 'attr_name': 'Loc.Ref.', 'attr_value': 'NZ3860014550'},
    {'sai_num': 'X001', 'attr_name': 'Specific Model/Frame', 'attr_value': 'Hydro1'},
    {'sai_num': 'X001', 'attr_name': 'Serial No', 'attr_value': 'XYZ001'},
    {'sai_num': 'X001', 'attr_name': 'P AND I Tag No', 'attr_value': 'INS01'},
    {'sai_num': 'X001', 'attr_name': 'Weight kg', 'attr_value': '10.0'},
])


def extract_masterdata(df: pl.DataFrame) -> pl.DataFrame: 
    return df.select(
        [ (pl.col("sai_num"))
        , (pl.col("Common Name").str.extract(r"/([^/]+)/EQUIPMENT:", 1).alias("decription"))
        , (pl.col("Common Name").str.extract(r"EQUIPMENT: (.+)", 1).alias("equi_type"))
        , (pl.col("Manufacturer").alias("manufacturer"))
        , (pl.col("Model").alias("model"))
        , (pl.col("Specific Model/Frame").alias("manuf_part_no"))
        , (pl.col("Serial No").alias("serial_number"))
        , (pl.col("Weight kg").alias("weight"))
        , (pl.when(pl.col("Weight kg").is_not_null().or_(pl.col("Weight kg") != ""))
            .then(pl.lit("KG"))
            .otherwise(pl.lit(""))
            .alias("weight_unit"))
        , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%d.%m.%Y")).alias("startup_date")
        , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%Y")).alias("constr_year")
        , (pl.col("Installed From").str.to_datetime("%m/%d/%y %H:%M").dt.to_string("%m")).alias("constr_month")
        ]
    )



equipment_ddl = """
    CREATE OR REPLACE TABLE ai2_to_s4.equipment  (
        equi VARCHAR NOT NULL,
        description VARCHAR,
        manufacturer VARCHAR,
        model VARCHAR,
        PRIMARY KEY(equi)
    );
    """


def equipment_insert(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_to_s4.equipment BY NAME
    SELECT 
        df.sai_num AS equi,
        df.decription AS description,
        df.manufacturer AS manufacturer,
        df.model AS model,
    FROM {df_view_name} AS df
    WHERE df.sai_num <> 'X001'
    ON CONFLICT DO NOTHING;
    """

def store_masterdata(*, con: duckdb.DuckDBPyConnection, exec_ddl: bool, df: pl.DataFrame) -> None:
    con.register(view_name='vw_masterdata_df', python_object=df)
    if exec_ddl:
        con.execute(equipment_ddl)
    con.execute(equipment_insert(df_view_name="vw_masterdata_df"))

