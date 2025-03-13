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



import polars as pl
import duckdb

# import_utils is now too complicated

# Don't expose a dataframe or a dataframe-to-datframe transform 
# function in the interface.
# For csv use DuckDB directly
# Dont use XlsxSource
# Hopefully DuckDB's Excel extension will be expanded to make this
# module redundant


# Analogue of
# CREATE TABLE <qual_table_name> AS SELECT * FROM read_xlsx(<file_path>, <sheet_name>);
#
def create_table_xlsx(*, 
                      qualified_table_name: str,
                      file_path: str, 
                      sheet_name: str,
                      con: duckdb.DuckDBPyConnection,
                      select_spec: str | None = None,
                      where_spec: str | None = None) -> None:
    select_spec = select_spec if select_spec else "*"
    where_spec = f'WHERE {where_spec}' if where_spec else ""
    df1 = pl.read_excel(source=file_path, sheet_name=sheet_name, engine='calamine')
    con.register(view_name='vw_df1', python_object=df1)
    sql_stmt = f'CREATE OR REPLACE TABLE {qualified_table_name} AS SELECT {select_spec} FROM vw_df1 {where_spec};'
    con.execute(sql_stmt)
    con.commit()

# Analogue of
# INSERT INTO <qual_table_name> BY NAME AS SELECT <select_spec> FROM read_xlsx(<file_path>, <sheet_name>);
#
def insert_into_by_name_xlsx(*, 
                             qualified_table_name: str,
                             file_path: str, 
                             sheet_name: str,
                             con: duckdb.DuckDBPyConnection, 
                             select_spec: str | None = None, 
                             where_spec: str | None = None) -> None:
    select_spec = select_spec if select_spec else "*"
    where_spec = f'WHERE {where_spec}' if where_spec else ""
    df1 = pl.read_excel(source=file_path, sheet_name=sheet_name, engine='calamine')
    con.register(view_name='vw_df1', python_object=df1)
    sql_stmt = f'INSERT INTO {qualified_table_name} BY NAME SELECT {select_spec} FROM vw_df1 {where_spec};'
    con.execute(sql_stmt)
    con.commit()
