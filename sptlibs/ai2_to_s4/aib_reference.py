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


def get_parents(*, con: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    return con.execute("""
        SELECT t.pli_num AS pli_num, t.sai_num AS sai_num FROM ai2_to_s4.vw_parent_sai_numbers AS t;
        """
    ).pl()


def extract_chars(*, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
    df = get_parents(con=con)
    return df



aib_reference_ddl = """
    CREATE OR REPLACE TABLE ai2_to_s4.aib_reference  (
        equi VARCHAR,
        ai2_aib_reference VARCHAR[],
        s4_aib_reference VARCHAR,
        PRIMARY KEY(equi)
    );
    """


def aib_reference_insert(*, df_view_name: str) -> str: 
    return f"""
    INSERT INTO ai2_to_s4.aib_reference BY NAME
    SELECT 
        df.pli_num AS equi,
        list_value(df.pli_num, df.sai_num) AS ai2_aib_reference,
    FROM {df_view_name} AS df
    WHERE df.sai_num <> 'X001'
    ON CONFLICT DO NOTHING;
    """

def store_class(*, con: duckdb.DuckDBPyConnection, exec_ddl: bool, df: pl.DataFrame) -> None:
    con.register(view_name='vw_aib_reference_df', python_object=df)
    if exec_ddl:
        con.execute(aib_reference_ddl)
    con.execute(aib_reference_insert(df_view_name="vw_aib_reference_df"))

