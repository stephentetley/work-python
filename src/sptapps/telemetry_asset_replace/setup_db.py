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
import polars as pl
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptlibs.data_access.excel_table.excel_table_import as excel_table_import

def init_db(*, 
            worklist_path: str,
            sheet_name: str, 
            con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='setup_tables.sql')
    excel_table_import.duckdb_import(xls_path=worklist_path, 
                                     con=con, 
                                     table_name='telemetry_landing.worklist', 
                                     schema_overrides={"S4 Floc": pl.String, "Comment": pl.String},
                                     sheet_name=sheet_name)
    

def fill_db(*,
            create_header: str,
            create_notes: list[str],
            change_header: str,
            change_notes: list[str],
            con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='fill_s4_equi_masterdata.sql')
    runner.exec_sql_file(rel_file_path='fill_s4_equi_classes.sql')
    runner.exec_sql_file(rel_file_path='equi_masterdata_to_excel_uploader.sql')
    runner.exec_sql_file(rel_file_path='equi_classes_to_excel_uploader.sql')
    runner.exec_sql_file(rel_file_path='fill_disposed_of.sql')
    _add_create_header(create_header, con)
    _add_create_notes(create_notes, con)
    _add_change_header(change_header, con)
    _add_change_notes(change_notes, con)

def _add_create_header(cr_header: str, con: duckdb.DuckDBPyConnection) -> None:
    stmt = f"INSERT INTO excel_uploader_equi_create.change_request_header VALUES (null, '{cr_header}');"
    con.execute(stmt)

def _add_create_notes(notes: list[str], con: duckdb.DuckDBPyConnection) -> None:
    con.execute("BEGIN TRANSACTION;")
    for note in notes:  
        stmt = f"INSERT INTO excel_uploader_equi_create.change_request_notes VALUES ('{note}');"
        con.execute(stmt)               
    con.execute("COMMIT;")


def _add_change_header(cr_header: str, con: duckdb.DuckDBPyConnection) -> None:
    stmt = f"INSERT INTO excel_uploader_equi_change.change_request_header VALUES (null, '{cr_header}');"
    con.execute(stmt)

def _add_change_notes(notes: list[str], con: duckdb.DuckDBPyConnection) -> None:
    con.execute("BEGIN TRANSACTION;")
    for note in notes:  
        stmt = f"INSERT INTO excel_uploader_equi_change.change_request_notes VALUES ('{note}');"
        con.execute(stmt)               
    con.execute("COMMIT;")
