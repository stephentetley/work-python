"""
Copyright 2023 Stephen Tetley

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

import re
import polars as pl
import duckdb
from typing import Callable
from sptlibs.utils.xlsx_source import XlsxSource

def read_csv_source(csv_path: str, *, normalize_column_names: bool, has_header: bool) -> pl.DataFrame:
    df = pl.read_csv(source=csv_path, ignore_errors=True, has_header=has_header, null_values = ['NULL', 'Null', 'null'])
    if normalize_column_names:
        return normalize_df_column_names(df)
    else:
        return df


def duckdb_import_csv(
        csv_path: str, 
        *, table_name: str, 
        rename_before_trafo: bool, 
        df_trafo: Callable[[pl.DataFrame], pl.DataFrame], 
        con: duckdb.DuckDBPyConnection) -> None:
    '''Note drops the table `table_name` before filling it. Must have headers.'''
    df_raw = pl.read_csv(source=csv_path, ignore_errors=True, has_header=True, null_values = ['NULL', 'Null', 'null'])
    if rename_before_trafo: 
        df1 = df_renamed = normalize_df_column_names(df_raw)
    else: 
        df1 = df_raw
    if df_trafo is not None:
        df_clean = df_trafo(df1)
    else:
        df_clean = df1
    if not rename_before_trafo: 
        df2 = df_renamed = normalize_df_column_names(df_clean)
    else:
        df2 = df_clean
    con.register(view_name='vw_df_renamed', python_object=df2)
    sql_stmt = f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM vw_df_renamed;'
    con.execute(sql_stmt)
    con.commit()


def duckdb_import_cvs_into(csv_path: str, *, df_name: str, insert_stmt: str, con: duckdb.DuckDBPyConnection, df_trafo: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
    '''Fill a table with an INSERT INTO statement'''
    df_raw = pl.read_csv(source=csv_path, ignore_errors=True, has_header=True, null_values = ['NULL', 'Null', 'null'])
    if df_trafo is not None:
        df_clean = df_trafo(df_raw)
    else:
        df_clean = df_raw
    df_renamed = normalize_df_column_names(df_clean)
    # print(df_renamed.columns)
    con.register(view_name=df_name, python_object=df_renamed)
    con.execute(insert_stmt)
    con.commit()


# 'xlsx2csv' is the fastest engine
def read_xlsx_source(source: XlsxSource, *, normalize_column_names: bool) -> pl.DataFrame:
    df = pl.read_excel(source=source.path, sheet_name=source.sheet, engine='xlsx2csv', read_csv_options = {'ignore_errors': True, 'null_values': ['NULL', 'Null', 'null']})
    if normalize_column_names:
        return normalize_df_column_names(df)
    else:
        return df


def duckdb_import_sheet(source: XlsxSource, *, table_name: str, con: duckdb.DuckDBPyConnection, df_trafo: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
    '''Note drops the table `table_name` before filling it'''
    df_raw = pl.read_excel(source=source.path, sheet_name=source.sheet, engine='xlsx2csv', read_csv_options = {'ignore_errors': True, 'null_values': ['NULL', 'Null', 'null']})
    if df_trafo is not None:
        df_clean = df_trafo(df_raw)
    else:
        df_clean = df_raw
    df_renamed = normalize_df_column_names(df_clean)
    con.register(view_name='vw_df_renamed', python_object=df_renamed)
    sql_stmt = f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM vw_df_renamed;'
    con.execute(sql_stmt)
    con.commit()

def duckdb_import_sheet_into(source: XlsxSource, *, df_name: str, insert_stmt: str, con: duckdb.DuckDBPyConnection, df_trafo: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
    '''Fill a table with an INSERT INTO statement'''
    df_raw = pl.read_excel(source=source.path, sheet_name=source.sheet, engine='xlsx2csv', read_csv_options = {'ignore_errors': True, 'null_values': ['NULL', 'Null', 'null']})
    if df_trafo is not None:
        df_clean = df_trafo(df_raw)
    else:
        df_clean = df_raw
    df_renamed = normalize_df_column_names(df_clean)
    # print(df_renamed.columns)
    con.register(view_name=df_name, python_object=df_renamed)
    con.execute(insert_stmt)
    con.commit()


def normalize_df_column_names(df: pl.DataFrame) -> pl.DataFrame:
    new_names = {}
    for name in df.columns:
        new_names[name] = normalize_name(name)
    return df.rename(new_names)


def normalize_name(s: str) -> str:
    ls = s.lower()
    replace_non_w = re.sub(pattern=r'[\W]+' , repl=' ', string=ls)
    trimmed = replace_non_w.strip()
    remove_spaces = re.sub(pattern=r'[\W]+' , repl='_', string=trimmed)
    return remove_spaces

def remove_df_column_name_indices(df: pl.DataFrame) -> pl.DataFrame:
    new_names = {}
    for name in df.columns:
        new_names[name] = remove_index(name)
    return df.rename(new_names)

def remove_index(s: str) -> str:
    drop_ix = re.sub(pattern=r'_[\d]+$' , repl='', string=s)
    return drop_ix

