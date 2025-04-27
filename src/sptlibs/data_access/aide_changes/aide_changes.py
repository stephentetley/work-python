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

def duckdb_init(*, xlsx_directory_glob: str, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='aide_changes_setup.sql')
    query = f"""
        INSERT INTO aide_changes_landing.landing_files BY NAME
        SELECT 
            'aide_changes_landing.export' || file_index AS table_name,
            file_name AS file_name
        FROM get_glob_matches('{xlsx_directory_glob}');
        """
    con.execute(query)
    query = f"SELECT * FROM get_glob_matches('{xlsx_directory_glob}');"
    df = con.execute(query).pl()
    for row in df.rows(named=True):
        idx = row['file_index']
        xlsx_file_path = row['file_path']
        xlsx_file = row['file_name']
        insert_stmt = f"""
            CREATE OR REPLACE TABLE aide_changes_landing.export{idx} AS 
            SELECT 
                '{xlsx_file}' AS source_file,
                t.* EXCLUDE("Requested By") FROM read_xlsx('{xlsx_file_path}') t;
            """
        con.execute(insert_stmt)
    query = f"SELECT * FROM aide_changes_landing.vw_landing_tables;"
    df = con.execute(query).pl()
    for row in df.rows(named=True):
        table_name = row['qualified_table_name']
        insert_stmt = f"""
            INSERT INTO aide_changes.change_export_all BY NAME 
            SELECT * FROM get_changes_from_export('{table_name}');
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



