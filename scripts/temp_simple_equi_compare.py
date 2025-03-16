# temp_simple_equi_compare.py

import os
import duckdb
import sptapps.simple_equi_compare.generate_report as generate_report
import sptlibs.data_access.ai2_metadata.process_processgroup_names_import as process_processgroup_names_import
import sptlibs.data_access.ai2_metadata.site_mapping_import as site_mapping_import
import sptlibs.data_access.import_utils2 as import_utils2


duckdb_path = 'E:/coding/work/soev_equi_compare.duckdb'
ppg_names_path = 'G:/work/2025/asset_data_facts/all_process_processgroup_names.xlsx'
site_mapping_path = 'G:/work/2025/asset_data_facts/SiteMapping.xlsx'

ai2_soev_glob_path = 'G:/work/2025/storm_overflow_reconcile/ai2_export*.xlsx'
s4_soev_path1 = 'G:/work/2025/storm_overflow_reconcile/ih08_lstn_export1.with_aib_reference.xlsx'
s4_soev_path2 = 'G:/work/2025/storm_overflow_reconcile/ih08_fstn_export1.with_aib_reference.xlsx'


# Delete duckdb_path first   
if os.path.exists(duckdb_path):
    os.remove(duckdb_path)


con = duckdb.connect(database=duckdb_path, read_only=False)
con.execute('INSTALL excel;')
con.execute('LOAD excel;')
generate_report.duckdb_init(con=con)
process_processgroup_names_import.duckdb_import(xlsx_path=ppg_names_path, con=con)
site_mapping_import.duckdb_import(xlsx_path=site_mapping_path, con=con)
ai2_names = import_utils2.df_create_tables_xlsx(pathname=ai2_soev_glob_path, 
                                             sheet_name='Sheet1',
                                             qualified_table_name='equi_raw_data.ai2_export',
                                             con=con)
import_utils2.insert_union_by_name_into(qualified_table_name='equi_compare.ai2_equipment',
                                        or_replace=True,
                                        extractor_table_function='extract_ai2_equi_data_from_raw',
                                        source_tables=ai2_names,
                                        con=con)
print("here")
s4_names = import_utils2.df_create_table_xlsx(pathname=s4_soev_path1, 
                                                  sheet_name='Sheet1',
                                                  qualified_table_name='equi_raw_data.s4_export1',
                                                  slice_size = 2000,
                                                  select_spec='* EXCLUDE("Selected Line", "Superord. Equipment", "Function class", "Class AIB_REFERENCE is assigned")',
                                                  con=con)
con.close()
con = duckdb.connect(database=duckdb_path, read_only=False)

s4_names = import_utils2.df_create_table_xlsx(pathname=s4_soev_path2, 
                                                  sheet_name='Sheet1',
                                                  qualified_table_name='equi_raw_data.s4_export2',
                                                  slice_size = 2000,
                                                  select_spec='* EXCLUDE("Selected Line", "Superord. Equipment", "Function class", "Class AIB_REFERENCE is assigned")',
                                                  con=con)

import_utils2.insert_union_by_name_into(qualified_table_name='equi_compare.s4_equipment',
                                        or_replace=True,
                                        extractor_table_function='extract_s4_equi_data_from_raw',
                                        source_tables=['equi_raw_data.s4_export1', 'equi_raw_data.s4_export2'],
                                        con=con)

con.close()

print(f"Done - created: {duckdb_path}")

print(ai2_names)
print(s4_names)


# Need to expose DuckDB EXCEL loading for large tables.