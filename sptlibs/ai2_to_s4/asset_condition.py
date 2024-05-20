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
    {'sai_num': 'X001', 'attr_name': 'Condition Grade', 'attr_value': '1 - Good'},
    {'sai_num': 'X001', 'attr_name': 'Condition Grade Reason', 'attr_value': 'New'},
    {'sai_num': 'X001', 'attr_name': 'AGASP Survey Year', 'attr_value': '2019'},
])


def extract_chars(*, df: pl.DataFrame) -> pl.DataFrame: 
    return df.select(
        [ (pl.col("sai_num"))
        , (pl.col("Condition Grade").str.to_uppercase().alias("condition_grade"))
        , (pl.col("Condition Grade Reason").str.to_uppercase().alias("condition_grade_reason"))
        , (pl.col("AGASP Survey Year").cast(pl.Int32).alias("survey_date"))
        ]
    )



asset_condition_ddl = """
    CREATE OR REPLACE TABLE ai2_to_s4.asset_condition (
        equi VARCHAR NOT NULL,
        condition_grade VARCHAR,
        condition_grade_reason VARCHAR,
        survey_date INTEGER,
        PRIMARY KEY(equi)
    );
    """


def asset_condition_insert(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_to_s4.asset_condition BY NAME
    SELECT 
        df.sai_num AS equi,
        df.condition_grade AS condition_grade,
        df.condition_grade_reason AS condition_grade_reason,
        df.survey_date AS survey_date,
    FROM {df_view_name} AS df
    WHERE df.sai_num <> 'X001'
    AND df.survey_date IS NOT NULL
    ON CONFLICT DO NOTHING;
    """

def store_class(*, con: duckdb.DuckDBPyConnection, exec_ddl: bool, df: pl.DataFrame) -> None:
    con.register(view_name='vw_asset_condition_df', python_object=df)
    if exec_ddl:
        con.execute(asset_condition_ddl)
    con.execute(asset_condition_insert(df_view_name="vw_asset_condition_df"))

