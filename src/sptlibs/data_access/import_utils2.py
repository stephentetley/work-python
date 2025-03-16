"""
Copyright 2025 Stephen Tetley

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


import glob
import polars as pl
import duckdb
from typing import Callable


# import_utils is now too complicated

# Don't expose a dataframe or a dataframe-to-datframe transform 
# function in the interface.
# For csv use DuckDB directly
# Dont use XlsxSource
# Hopefully DuckDB's Excel extension will be expanded to make this
# module redundant

# Warning
# Inserting (>32,768) rows appears to cause a memory corruption issue
# Potentially sidestepped by closing and reopening the connection...
# Slicing doesn't seem to help...


# Analogue of
# CREATE TABLE <qual_table_name> AS SELECT * FROM read_xlsx(<pathname>, <sheet_name>);
# Use Polars for reading
def df_create_table_xlsx(*, 
                      qualified_table_name: str,
                      pathname: str, 
                      sheet_name: str,
                      con: duckdb.DuckDBPyConnection,
                      select_spec: str | None = None,
                      where_spec: str | None = None,
                      slice_size: int = 0) -> None:
    name1 = qualified_table_name.replace('.', '_')
    view_name = f'vw_df_{name1}'
    select_spec = select_spec if select_spec else "*"
    where_spec = f'WHERE {where_spec}' if where_spec else ""
    dfall = pl.read_excel(source=pathname, sheet_name=sheet_name, engine='calamine')
    slice_size = dfall.height if slice_size <= 0 else slice_size
    df1 = dfall.slice(0, slice_size)
    con.register(view_name=view_name, python_object=df1)
    sql_stmt = f'CREATE OR REPLACE TABLE {qualified_table_name} AS SELECT {select_spec} FROM {view_name} {where_spec};'
    con.execute(sql_stmt)
    con.unregister(view_name=view_name)
    con.commit()
    start = slice_size
    batch = 1
    while start < dfall.height:
        view_name = f'vw_df_{batch}'
        df1 = dfall.slice(start, slice_size)
        con.register(view_name=view_name, python_object=df1)
        sql_stmt = f"""
            INSERT INTO {qualified_table_name} BY NAME 
            SELECT {select_spec} FROM {view_name} {where_spec};
        """
        con.execute(sql_stmt)
        con.unregister(view_name=view_name)
        con.commit()
        start = start + slice_size
        batch = batch + 1



# Warpper over
# CREATE TABLE <qual_table_name> AS SELECT * FROM read_xlsx(<pathname>, <sheet_name>);
# Must use DuckDB > 1.20 and excel extension must be loaded
def create_table_xlsx(*, 
                      qualified_table_name: str,
                      pathname: str, 
                      sheet_name: str,
                      con: duckdb.DuckDBPyConnection,
                      select_spec: str | None = None,
                      where_spec: str | None = None) -> None:
    select_spec = select_spec if select_spec else "*"
    where_spec = f'WHERE {where_spec}' if where_spec else ""
    sql_stmt = f"""
        CREATE OR REPLACE TABLE {qualified_table_name} AS 
        SELECT {select_spec} FROM read_xlsx('{pathname}', sheet='{sheet_name}') {where_spec};
    """
    con.execute(sql_stmt)
    con.commit()


# Version of `create_table_xlsx` that creates multiple tables from the files 
# matching the glob. Table name is suffixed with the running count and a 
# list of the created tablenames is returned.
#
def df_create_tables_xlsx(*, 
                          qualified_table_name: str,
                          pathname: str, 
                          sheet_name: str,
                          con: duckdb.DuckDBPyConnection,
                          select_spec: str | None = None,
                          where_spec: str | None = None,
                          slice_size: int = 0) -> list[str]:

    def not_temp(file_name): 
        return not '~$' in file_name
    globlist = filter(not_temp, glob.glob(pathname=pathname))
    def create_table(ix, pathname): 
        table_name = f'{qualified_table_name}{ix}'
        df_create_table_xlsx(qualified_table_name=table_name,
                             pathname=pathname,
                             sheet_name=sheet_name,
                             con=con,
                             select_spec=select_spec,
                             where_spec=where_spec,
                             slice_size=slice_size)
        return table_name
    return [create_table(ix+1, file) for ix, file in enumerate(globlist)]

# Needs duckdb >= 1.2.0 to use qualified table names in table macros...
def insert_union_by_name_into(*, 
                             or_replace: bool = False,
                             qualified_table_name: str,
                             extractor_table_function: str, 
                             source_tables: list[str],
                             con: duckdb.DuckDBPyConnection,) -> None:
    or_replace_spec = "OR REPLACE" if or_replace else ""
    select_statements = [f"(SELECT * FROM {extractor_table_function}('{table_name}'))" for table_name in source_tables]
    sql_body = '\nUNION\n'.join(select_statements)
    sql_stmt = f'INSERT {or_replace_spec} INTO {qualified_table_name} BY NAME\n{sql_body}\n;'
    # print(sql_stmt)
    con.execute(sql_stmt)
    con.commit()


# Analogue of
# INSERT INTO <qual_table_name> BY NAME AS SELECT <select_spec> FROM read_xlsx(<pathname>, <sheet_name>);
#
def insert_into_by_name_xlsx(*, 
                             qualified_table_name: str,
                             pathname: str, 
                             sheet_name: str,
                             con: duckdb.DuckDBPyConnection, 
                             select_spec: str | None = None, 
                             where_spec: str | None = None) -> None:
    name1 = qualified_table_name.replace('.', '_')
    view_name = f'vw_df_{name1}'
    select_spec = select_spec if select_spec else "*"
    where_spec = f'WHERE {where_spec}' if where_spec else ""
    df1 = pl.read_excel(source=pathname, sheet_name=sheet_name, engine='calamine')
    con.register(view_name=view_name, python_object=df1)
    sql_stmt = f'INSERT INTO {qualified_table_name} BY NAME SELECT {select_spec} FROM {view_name} {where_spec};'
    con.execute(sql_stmt)
    con.commit()
