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
import sptlibs.data_access.s4_ztables.s4_ztables_import as s4_ztables_import
import sptlibs.data_access.excel_table.excel_table_import as excel_table_import
import sptlibs.data_access.s4_uploader.s4_uploader_export as s4_uploader_export
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def generate_flocs(*, worklist_path: str, 
                   ih06_path: str, 
                   ztable_source_db: str, 
                   uploader_template: str, 
                   uploader_outfile: str, 
                   con: duckdb.DuckDBPyConnection) -> None: 
    s4_ztables_import.copy_ztable_tables(source_db_path=ztable_source_db, dest_con=con)
    excel_table_import.duckdb_import(xls_path=worklist_path, sheet_name='Flocs', table_name='raw_data.worklist', con=con)
    excel_table_import.duckdb_import(xls_path=worklist_path, sheet_name='Config', table_name='raw_data.config', con=con)
    excel_table_import.duckdb_import(xls_path=ih06_path, sheet_name='Sheet1', table_name='raw_data.ih06_export', con=con)
    s4_uploader_export.setup_tables(con=con)
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='floc_delta_init_tables.sql')
    runner.exec_sql_file(rel_file_path='s4_uploader_insert_into.sql')
    s4_uploader_export.write_excel(upload_template_path=uploader_template,
                                   dest=uploader_outfile,
                                   con=con)
