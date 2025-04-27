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

import os
import duckdb
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def duckdb_init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='file_download_create_tables.sql')
    runner.exec_sql_file(rel_file_path='file_download_create_macros.sql')

def duckdb_import_files(*, file_paths: list[str], con: duckdb.DuckDBPyConnection) -> None: 
    for idx, path in enumerate(file_paths):
        file_name = os.path.basename(path)
        query = f"""
            INSERT INTO file_download_landing.landing_files BY NAME
            SELECT 
                'file_download_landing.export{idx+1}' AS qualified_table_name,
                '{file_name}' AS file_name,
                '{path}' AS file_path;
            """
        con.execute(query)
    _import_landing_tables(con=con)
    _translate_landing_tables(con=con)


def duckdb_import_directory(*, directory_glob: str, con: duckdb.DuckDBPyConnection) -> None: 
    query = f"""
        INSERT INTO file_download_landing.landing_files BY NAME
        SELECT 
            'file_download_landing.export' || file_index AS qualified_table_name,
            file_name AS file_name,
            file_path AS file_path,
        FROM get_glob_matches('{directory_glob}');
        """
    con.execute(query)
    _import_landing_tables(con=con)
    _translate_landing_tables(con=con)


def _import_landing_tables(*, con: duckdb.DuckDBPyConnection) -> None: 
    query = "SELECT * FROM file_download_landing.landing_files;"
    df = con.execute(query).pl()
    for row in df.rows(named=True):
        qualified_table_name = row['qualified_table_name']
        fd_file_path = row['file_path']
        insert_stmt = f"""
            CREATE OR REPLACE TABLE {qualified_table_name} AS 
            SELECT 
                t.* FROM get_raw_download_table('{fd_file_path}') t;
            """
        con.execute(insert_stmt)


def _translate_landing_tables(*, con: duckdb.DuckDBPyConnection) -> None: 
    query = "SELECT * FROM file_download_landing.vw_table_information;"
    df = con.execute(query).pl()
    for row in df.rows(named=True):
        qualified_landing_table = row['qualified_landing_table']
        entity_type = row['entity_type']
        insert_stmt = f"""
            INSERT INTO file_download.{entity_type} BY NAME 
            SELECT * FROM get_{entity_type}_landing_table('{qualified_landing_table}');
            """
        con.execute(insert_stmt)






