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


import duckdb
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def duckdb_init(*, directory_glob: str, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='file_download_create_tables.sql')
    runner.exec_sql_file(rel_file_path='file_download_create_macros.sql')
    query = f"""
        INSERT INTO file_download_landing.landing_files BY NAME
        SELECT 
            'file_download_landing.export' || file_index AS qualified_table_name,
            file_name AS file_name,
            file_path AS file_path,
        FROM get_glob_matches('{directory_glob}');
        """
    con.execute(query)
    query = f"SELECT * FROM get_glob_matches('{directory_glob}');"
    df = con.execute(query).pl()
    for row in df.rows(named=True):
        idx = row['file_index']
        fd_file_path = row['file_path']
        insert_stmt = f"""
            CREATE OR REPLACE TABLE file_download_landing.file_download{idx} AS 
            SELECT 
                t.* FROM get_raw_download_table('{fd_file_path}') t;
            """
        con.execute(insert_stmt)
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



def export_xlsx(*, xlsx_path: str, con: duckdb.DuckDBPyConnection) -> None: 
    query = f"""
        COPY (
            SELECT * FROM aide_changes.change_export_all
            ORDER BY change_date, source_file
        )
        TO '{xlsx_path}' WITH (FORMAT 'xlsx', HEADER true);
    """
    con.execute(query=query)



