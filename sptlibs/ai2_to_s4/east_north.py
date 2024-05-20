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
import sptlibs.utils.gridref as gridref


eav_sample = pl.DataFrame([ 
    {'sai_num': 'X001', 'attr_name': 'Loc.Ref.', 'attr_value': 'SD1234512345'},
])


def extract_chars(*, df: pl.DataFrame) -> pl.DataFrame: 
    return df.select(
        [ (pl.col("sai_num"))
        , (pl.col("Loc.Ref.").map_elements(lambda s: gridref.to_east_north(s)["easting"]).alias("easting"))
        , (pl.col("Loc.Ref.").map_elements(lambda s: gridref.to_east_north(s)["northing"]).alias("northing"))
        ]
    )



east_north_ddl = """
    CREATE OR REPLACE TABLE ai2_to_s4.east_north  (
        equi VARCHAR,
        easting INTEGER,
        northing INTEGER,
        PRIMARY KEY(equi)
    );
    """


def east_north_insert(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_to_s4.east_north BY NAME
    SELECT 
        df.sai_num AS equi,
        df.easting AS easting,
        df.northing AS northing,
    FROM {df_view_name} AS df
    WHERE df.sai_num <> 'X001'
    ON CONFLICT DO NOTHING;
    """

def store_class(*, con: duckdb.DuckDBPyConnection, exec_ddl: bool, df: pl.DataFrame) -> None:
    con.register(view_name='vw_east_north_df', python_object=df)
    if exec_ddl:
        con.execute(east_north_ddl)
    con.execute(east_north_insert(df_view_name="vw_east_north_df"))

