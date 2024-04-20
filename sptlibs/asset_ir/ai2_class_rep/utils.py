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

from typing import Callable
import polars as pl
import duckdb


def ingest_equipment_eav_data(
        *, 
        pivot_table_getter: Callable[[duckdb.DuckDBPyConnection], pl.DataFrame], 
        equipment_ai2_column_names: list[str], 
        extract_trafo: Callable[[pl.DataFrame], pl.DataFrame], 
        insert_stmt: str,
        df_view_name: str,
        con: duckdb.DuckDBPyConnection) -> None: 
    df = pivot_table_getter(con)
    df = assert_pivot_columns(equipment_ai2_column_names, df)
    df = extract_trafo(df)
    insert_class_rep_table(insert_stmt=insert_stmt, df_view_name=df_view_name, df=df, con=con)
    con.commit()


# def get_pivot_table(*, equipment_ai2_name: str, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
#     pivot_query = """
#         SELECT 
#             md.ai2_reference AS ai2_reference, 
#             pv.* EXCLUDE (ai2_reference)
#         FROM
#             ai2_export.master_data md
#         JOIN (PIVOT ai2_export.eav_data ON attribute_name USING first(attribute_value) GROUP BY ai2_reference) pv ON pv.ai2_reference = md.ai2_reference 
#         WHERE 
#             md.common_name LIKE ?
#         """
#     param = f"%EQUIPMENT: {equipment_ai2_name}"
#     df = con.execute(pivot_query, [param]).pl()
#     return df

def simple_pivot_getter(*, equipment_ai2_name: str) -> Callable[[duckdb.DuckDBPyConnection], pl.DataFrame]: 
    pivot_query = """
        SELECT 
            md.ai2_reference AS ai2_reference, 
            pv.* EXCLUDE (ai2_reference)
        FROM
            ai2_export.master_data md
        JOIN (PIVOT ai2_export.eav_data ON attribute_name USING first(attribute_value) GROUP BY ai2_reference) pv ON pv.ai2_reference = md.ai2_reference 
        WHERE 
            md.common_name LIKE ?
        """
    param = f"%EQUIPMENT: {equipment_ai2_name}"
    def getter(con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
        df = con.execute(pivot_query, [param]).pl()
        return df
    return getter

def assert_pivot_columns(colnames: list[str], df:pl.DataFrame) -> pl.DataFrame:
    cols = df.columns
    df1 = df
    for name in colnames:
        if not name in cols:
            df1 = df1.with_columns(pl.lit("").alias(name))
    return df1

def insert_class_rep_table(*, insert_stmt: str, df_view_name: str, df: pl.DataFrame, con: duckdb.DuckDBPyConnection) -> None:
    con.register(view_name=df_view_name, python_object=df)
    con.execute(insert_stmt)
    con.commit()
