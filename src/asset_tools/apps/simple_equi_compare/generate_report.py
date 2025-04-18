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
import sptlibs.asset_schema.udfs.setup_sql_udfs as setup_sql_udfs
import sptlibs.data_access.ai2_metadata.normalize_manuf_model_import as normalize_manuf_model_import
import sptlibs.data_access.ai2_metadata.process_processgroup_names_import as process_processgroup_names_import
import sptlibs.data_access.ai2_metadata.site_mapping_import as site_mapping_import
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptlibs.utils.export_utils as export_utils
import sptlibs.data_access.import_utils2 as import_utils2

def duckdb_init(*, 
                metadata_manuf_model_norm_path: str,
                metadata_ppg_names_path: str,
                metadata_site_mapping_path: str,
                s4_ih08_paths: list[str],
                ai2_export_paths: list[str],
                con: duckdb.DuckDBPyConnection) -> None:
    setup_sql_udfs.setup_udfx_macros(con=con) 
    con.execute('INSTALL excel;')
    con.execute('LOAD excel;')
    normalize_manuf_model_import.duckdb_import(xlsx_path=metadata_manuf_model_norm_path, con=con)
    process_processgroup_names_import.duckdb_import(xlsx_path=metadata_ppg_names_path, con=con)
    site_mapping_import.duckdb_import(xlsx_path=metadata_site_mapping_path, con=con)
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='equi_compare_create_tables.sql')
    runner.exec_sql_file(rel_file_path='split_common_name_create_macro.sql')
    runner.exec_sql_file(rel_file_path='extract_raw_data_create_macros.sql')
    s4_names = import_utils2.df_create_tables_list_xlsx(paths=s4_ih08_paths, 
                                                        sheet_name='Sheet1',
                                                        qualified_table_name='equi_raw_data.s4_export',
                                                        select_spec='* EXCLUDE("Selected Line", "Superord. Equipment", "Function class", "Class AIB_REFERENCE is assigned")',
                                                        con=con)

    import_utils2.insert_union_by_name_into(qualified_table_name='equi_compare.s4_equipment',
                                            or_replace=True,
                                            extractor_table_function='extract_s4_equi_data_from_raw',
                                            source_tables=s4_names,
                                            con=con)


    ai2_names = import_utils2.df_create_tables_list_xlsx(paths=ai2_export_paths, 
                                                         sheet_name='Sheet1',
                                                         qualified_table_name='equi_raw_data.ai2_export',
                                                         con=con)
    import_utils2.insert_union_by_name_into(qualified_table_name='equi_compare.ai2_equipment',
                                            or_replace=True,
                                            extractor_table_function='extract_ai2_equi_data_from_raw',
                                            source_tables=ai2_names,
                                            con=con)


output_query = '''
    SELECT 
        s4_site AS "S4 Site",
        pli_num AS "AI2 EquiId",
        s4_equi_id AS "S4 EuiId",
        record_status AS "Compare status",
        equipment_name AS "Equipment Name",
        functional_location AS "Functional Location", 
        techn_id_num AS "P and I Num",
        manfacturer AS "Manufacturer",
        model AS "Model",
        specific_model_frame AS "Specific Model/Frame",
        serial_number AS "Serial Number",
        install_date AS "Install date",
    FROM equi_compare.vw_compare_equi 
    ORDER BY s4_site, pli_num, record_status
'''

def gen_xls_report(*, xls_output_path: str, con: duckdb.DuckDBPyConnection) -> None:
    with xlsxwriter.Workbook(xls_output_path) as workbook:
        export_utils.write_sql_query_to_excel(
            select_query=output_query,
            workbook=workbook,
            sheet_name='equi_compare',
            con=con)