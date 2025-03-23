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
import xlsxwriter
import sptlibs.utils.export_utils as export_utils
import sptlibs.asset_schema.udfs.setup_sql_udfs as setup_sql_udfs
import sptlibs.data_access.excel_table.excel_table_import as excel_table_import
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def duckdb_init(*, 
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
    setup_sql_udfs.setup_macros(con=con)
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='raw_data_create_views.sql')
    runner.exec_sql_file(rel_file_path='aide_changes_create_tables.sql')
    runner.exec_sql_file(rel_file_path='aide_changes_insert_into.sql')
    


def gen_xls_report(*, xls_output_path: str, con: duckdb.DuckDBPyConnection) -> None:
    with xlsxwriter.Workbook(xls_output_path) as workbook:
        export_utils.write_sql_table_to_excel(
            qualified_table_name='aide_changes.vw_ai2_not_synced',
            order_by_columns=['common_name'],
            sheet_name='ai2_not_synced', 
            column_formats = {},
            con=con, workbook=workbook)
        export_utils.write_sql_table_to_excel(
            qualified_table_name='aide_changes.vw_s4_not_synced',
            order_by_columns=['functional_location'],
            sheet_name='s4_not_synced', 
            column_formats = {},
            con=con, workbook=workbook)
        export_utils.write_sql_table_to_excel(
            qualified_table_name='aide_changes.vw_s4_new',
            order_by_columns=['common_name'],
            sheet_name='s4_new', 
            column_formats = {},
            con=con, workbook=workbook)
        export_utils.write_sql_table_to_excel(
            qualified_table_name='aide_changes.vw_s4_changes',
            order_by_columns=['common_name'],
            sheet_name='s4_changes', 
            column_formats = {},
            con=con, workbook=workbook)        