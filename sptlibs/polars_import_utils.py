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
from sptlibs.xlsx_source import XlsxSource

# 'xlsx2csv' is the fastest engine

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

