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

import os
import random
import re
import glob
from typing import Callable
import polars as pl
import duckdb
from jinja2 import Template
from sptlibs.utils.xlsx_source import XlsxSource

def read_csv_source(
        csv_path: str, 
        *, 
        normalize_column_names = True, 
        has_header = True, 
        pre_normalize_names_trafo = None | Callable[[pl.DataFrame], pl.DataFrame],
        post_normalize_names_trafo = None | Callable[[pl.DataFrame], pl.DataFrame]) -> pl.DataFrame:
    df = pl.read_csv(source=csv_path, ignore_errors=True, has_header=has_header, null_values = ['NULL', 'Null', 'null'])
    if pre_normalize_names_trafo: 
        df = pre_normalize_names_trafo(df)
    if normalize_column_names:
        df = pre_normalize_names_trafo(df)
    if post_normalize_names_trafo: 
        df = post_normalize_names_trafo(df)
    return df

# returns a Polars data frame
# Note - probably not worth using this as we lose configuring `pl.excel_read`
def read_xlsx_source(
        source: XlsxSource, 
        *, 
        pre_normalize_names_trafo: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
        post_normalize_names_trafo: Callable[[pl.DataFrame], pl.DataFrame] | None = None,
        normalize_column_names = True) -> pl.DataFrame:
    df = pl.read_excel(source=source.path, sheet_name=source.sheet, engine='calamine', drop_empty_rows=True)
    if pre_normalize_names_trafo: 
        df = pre_normalize_names_trafo(df)
    if normalize_column_names:
        df = normalize_df_column_names(df)
    if post_normalize_names_trafo: 
        df = post_normalize_names_trafo(df)
    return df

def duckdb_write_dataframe_to_table(
        df:  pl.DataFrame, 
        *, 
        qualified_table_name: str, 
        con: duckdb.DuckDBPyConnection,
        columns_and_aliases: dict[str, str] = {} ) -> None:
    if not columns_and_aliases:
        columns = [{'column_name': x, 'alias_name': x} for x in df.columns]
    else:
        columns = [{'column_name': k, 'alias_name': v} for (k, v) in columns_and_aliases.items()]
    if columns: 
        df_view_name = f"vw_dataframe_{random.randint(1, 20000)}"
        con.register(view_name=df_view_name, python_object=df)
        sql_stmt = Template(_renaming_insert_stmt).render(qualified_table_name=qualified_table_name, df_view_name=df_view_name, columns=columns)
        con.execute(sql_stmt)
        con.commit()
    else:
        print(f"duckdb_store_dataframe - empty columns list...")


_renaming_insert_stmt = """
    INSERT INTO {{qualified_table_name}} BY NAME
    SELECT 
        {% for col in columns %}
        {{col.column_name }} AS {{col.alias_name}},
        {% endfor %}
    FROM 
        {{df_view_name}} df;
"""


def duckdb_import_tables_from_duckdb(
        *, 
        source_db_path: str, 
        con: duckdb.DuckDBPyConnection,
        source_tables: list[str],
        schema_name: str, 
        create_schama: bool = False) -> None:
    if create_schama: 
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
    con.execute(f"ATTACH '{source_db_path}' AS table_source;")
    for source_table in source_tables: 
        _, _, dest_name = source_table.partition('.')
        if dest_name: 
            con.execute(f"CREATE OR REPLACE TABLE {schema_name}.{dest_name} AS SELECT * FROM table_source.{source_table};")
    con.execute(f"DETACH table_source;")


def duckdb_store_polars_dataframe(
        df: pl.DataFrame, 
        *, 
        table_name: str, 
        con: duckdb.DuckDBPyConnection) -> None:
    '''Note drops the table `table_name` before filling it. Must have headers.'''
    con.register(view_name='vw_polars_df', python_object=df)
    sql_stmt = f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM vw_polars_df;'
    con.execute(sql_stmt)
    con.commit()


def duckdb_import_csv(
        csv_path: str, 
        *, 
        separator: str, 
        table_name: str, 
        rename_before_trafo: bool, 
        df_trafo: Callable[[pl.DataFrame], pl.DataFrame], 
        con: duckdb.DuckDBPyConnection) -> None:
    '''Note drops the table `table_name` before filling it. Must have headers.'''
    df_raw = pl.read_csv(source=csv_path, separator=separator, ignore_errors=True, has_header=True, null_values = ['NULL', 'Null', 'null'])
    if rename_before_trafo: 
        df1 = normalize_df_column_names(df_raw)
    else: 
        df1 = df_raw
    if df_trafo is not None:
        df_clean = df_trafo(df1)
    else:
        df_clean = df1
    if not rename_before_trafo: 
        df2 = normalize_df_column_names(df_clean)
    else:
        df2 = df_clean
    con.register(view_name='vw_df_renamed', python_object=df2)
    sql_stmt = f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM vw_df_renamed;'
    con.execute(sql_stmt)
    con.commit()


def duckdb_import_cvs_into(csv_path: str, *, separator: str, df_name: str, insert_stmt: str, con: duckdb.DuckDBPyConnection, df_trafo: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
    '''Fill a table with an INSERT INTO statement'''
    df_raw = pl.read_csv(source=csv_path, separator=separator, ignore_errors=True, has_header=True, null_values = ['NULL', 'Null', 'null'])
    if df_trafo is not None:
        df_clean = df_trafo(df_raw)
    else:
        df_clean = df_raw
    df_renamed = normalize_df_column_names(df_clean)
    # print(df_renamed.columns)
    con.register(view_name=df_name, python_object=df_renamed)
    con.execute(insert_stmt)
    con.commit()



def duckdb_import_sheet(source: XlsxSource, *, qualified_table_name: str, con: duckdb.DuckDBPyConnection, df_trafo: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
    '''Note drops the table `table_name` before filling it'''
    if source.sheet:
        df_raw = pl.read_excel(source=source.path, sheet_name=source.sheet, engine='calamine')
    else:
        df_raw = pl.read_excel(source=source.path, sheet_id=1, engine='calamine')
    if df_trafo is not None:
        df_clean = df_trafo(df_raw)
    else:
        df_clean = df_raw
    df_renamed = normalize_df_column_names(df_clean)
    con.register(view_name='vw_df_renamed', python_object=df_renamed)
    sql_stmt = f'CREATE OR REPLACE TABLE {qualified_table_name} AS SELECT * FROM vw_df_renamed;'
    con.execute(sql_stmt)
    con.commit()

def duckdb_import_sheet_into(source: XlsxSource, *, df_name: str, insert_stmt: str, con: duckdb.DuckDBPyConnection, df_trafo: Callable[[pl.DataFrame], pl.DataFrame]) -> None:
    '''Fill a table with an INSERT INTO statement'''
    df_raw = pl.read_excel(source=source.path, sheet_name=source.sheet, engine='calamine')
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
        new_names[name] = _remove_index(name)
    return df.rename(new_names)

def _remove_index(s: str) -> str:
    drop_ix = re.sub(pattern=r'_[\d]+$' , repl='', string=s)
    return drop_ix

def get_excel_sources_from_folder(source_folder: str, *, 
                                  sheet_name: str = 'Sheet1', 
                                  glob_pattern: str = '*.xlsx') -> list[XlsxSource]:
    globlist = glob.glob(glob_pattern, root_dir=source_folder, recursive=False)
    def not_temp(file_name): 
        return not '~$' in file_name
    def expand(file_name): 
        return XlsxSource(os.path.normpath(os.path.join(source_folder, file_name)), sheet_name)
    return [expand(e) for e in globlist if not_temp(e)]