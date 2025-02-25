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
import sptlibs.data_access.excel_table.excel_table_import as excel_table_import
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def setup_db(*, 
             equi_type_translation: str,
             aide_changelist: str,
             ai2_site_export: str,
             ih06_source: str,
             ih08_source: str,
             con: duckdb.DuckDBPyConnection) -> None: 
    excel_table_import.duckdb_import(equi_type_translation, table_name='equi_translation.equi_type_translation', sheet_name='ai2_to_s4', con=con)
    excel_table_import.duckdb_import(ih08_source, table_name='raw_data.ih08_equi', con=con)
    excel_table_import.duckdb_import(ih06_source, table_name='raw_data.ih06_flocs', con=con)
    excel_table_import.duckdb_import(ai2_site_export, table_name='raw_data.ai2_site_export', con=con)
    excel_table_import.duckdb_import(aide_changelist, table_name='raw_data.aide_changelist', con=con)
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='east_north_macro_create.sql')
    runner.exec_sql_file(rel_file_path='aide_triage_insert_into.sql')
    runner.exec_sql_file(rel_file_path='setup_aide_changes_views.sql')
    