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
import sptlibs.asset_schema.udfs.setup_sql_udfs as setup_sql_udfs
import sptlibs.data_access.s4_ztables.s4_ztables_import as s4_ztables_import
import sptlibs.data_access.excel_table.excel_table_import as excel_table_import
import sptlibs.data_access.excel_uploader.excel_uploader_export as excel_uploader_export
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def duckdb_init(*, 
                worklist_path: str, 
                ih06_paths: list[str], 
                ztable_source_db: str,  
                con: duckdb.DuckDBPyConnection) -> None: 
    con.execute("CREATE SCHEMA IF NOT EXISTS floc_delta_landing;")
    con.execute("INSTALL excel;")
    con.execute("LOAD excel;")
    setup_sql_udfs.setup_udfx_macros(con=con)
    s4_ztables_import.copy_ztable_tables(source_db_path=ztable_source_db, dest_con=con)
    import_stmt = f"""
        CREATE OR REPLACE TABLE floc_delta_landing.worklist AS 
        SELECT * FROM read_xlsx('{worklist_path}', header = true, sheet ='Flocs', all_varchar = true);
    """
    con.execute(import_stmt)
    excel_table_import.duckdb_imports(xls_paths=ih06_paths, 
                                      sheet_name='Sheet1', 
                                      table_name_root='floc_delta_landing.floc_export', 
                                      union=True,
                                      con=con)
    excel_uploader_export.duckdb_init_floc(con=con)
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='floc_delta_create_tables.sql')
    runner.exec_sql_file(rel_file_path='floc_delta_insert_into.sql')
    runner.exec_sql_file(rel_file_path='excel_uploader_insert_into.sql')

def gen_xls_upload(*, 
                   uploader_template: str, 
                   uploader_outfile: str,
                   con: duckdb.DuckDBPyConnection) -> None:
    excel_uploader_export.write_excel_floc(upload_template_path=uploader_template,
                                           dest=uploader_outfile,
                                           con=con)
