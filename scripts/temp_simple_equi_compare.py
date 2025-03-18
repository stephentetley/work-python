# temp_simple_equi_compare.py

import os
import duckdb
import xlsxwriter
import sptapps.simple_equi_compare.generate_report as generate_report
import sptlibs.data_access.ai2_metadata.process_processgroup_names_import as process_processgroup_names_import
import sptlibs.data_access.ai2_metadata.site_mapping_import as site_mapping_import
import sptlibs.data_access.import_utils2 as import_utils2
import sptlibs.utils.export_utils as export_utils

duckdb_path = 'E:/coding/work/soev_equi_compare.duckdb'
ppg_names_path = 'G:/work/2025/asset_data_facts/all_process_processgroup_names.xlsx'
manuf_model_norm_path = 'G:/work/2025/asset_data_facts/normalize_manuf_model.xlsx'
site_mapping_path = 'G:/work/2025/asset_data_facts/SiteMapping.xlsx'

ai2_soev_glob_path = 'G:/work/2025/storm_overflow_reconcile/ai2_export*.xlsx'
s4_soev_glob_path = 'G:/work/2025/storm_overflow_reconcile/ih08_*_export1.with_aib_reference.xlsx'

output_report = 'G:/work/2025/storm_overflow_reconcile/flow_level_analysis3.xlsx'


# Delete duckdb_path first   
if os.path.exists(duckdb_path):
    os.remove(duckdb_path)


con = duckdb.connect(database=duckdb_path, read_only=False, config={'memory_limit':'1GB'})
con.execute('INSTALL excel;')
con.execute('LOAD excel;')
generate_report.duckdb_init(manuf_model_norm_path=manuf_model_norm_path, con=con)
process_processgroup_names_import.duckdb_import(xlsx_path=ppg_names_path, con=con)
site_mapping_import.duckdb_import(xlsx_path=site_mapping_path, con=con)

s4_names = import_utils2.df_create_tables_xlsx(pathname=s4_soev_glob_path, 
                                                  sheet_name='Sheet1',
                                                  qualified_table_name='equi_raw_data.s4_export',
                                                  select_spec='* EXCLUDE("Selected Line", "Superord. Equipment", "Function class", "Class AIB_REFERENCE is assigned")',
                                                  con=con)

import_utils2.insert_union_by_name_into(qualified_table_name='equi_compare.s4_equipment',
                                        or_replace=True,
                                        extractor_table_function='extract_s4_equi_data_from_raw',
                                        source_tables=s4_names,
                                        con=con)


ai2_names = import_utils2.df_create_tables_xlsx(pathname=ai2_soev_glob_path, 
                                                sheet_name='Sheet1',
                                                qualified_table_name='equi_raw_data.ai2_export',
                                                con=con)
import_utils2.insert_union_by_name_into(qualified_table_name='equi_compare.ai2_equipment',
                                        or_replace=True,
                                        extractor_table_function='extract_ai2_equi_data_from_raw',
                                        source_tables=ai2_names,
                                        con=con)

with xlsxwriter.Workbook(output_report) as workbook:
    export_utils.write_sql_query_to_excel(
        select_query="SELECT * FROM equi_compare.vw_compare_equi ORDER BY s4_site, pli_num",
        workbook=workbook,
        sheet_name='equi_compare',
        con=con)
    
con.close()

print(s4_names)
print(ai2_names)
print(f"Done - created: {output_report}")

