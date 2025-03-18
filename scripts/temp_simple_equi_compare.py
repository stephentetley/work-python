# temp_simple_equi_compare.py

import os
import duckdb
import xlsxwriter
import sptapps.simple_equi_compare.generate_report as generate_report

duckdb_path = 'E:/coding/work/soev_equi_compare.duckdb'
ppg_names_path = 'G:/work/2025/asset_data_facts/all_process_processgroup_names.xlsx'
manuf_model_norm_path = 'G:/work/2025/asset_data_facts/normalize_manuf_model.xlsx'
site_mapping_path = 'G:/work/2025/asset_data_facts/SiteMapping.xlsx'

ai2_soev_glob_path = 'G:/work/2025/storm_overflow_reconcile/ai2_export*.xlsx'
s4_soev_glob_path = 'G:/work/2025/storm_overflow_reconcile/ih08_*_export1.with_aib_reference.xlsx'

output_report = 'G:/work/2025/storm_overflow_reconcile/flow_level_analysis4.xlsx'


# Delete duckdb_path first   
if os.path.exists(duckdb_path):
    os.remove(duckdb_path)


con = duckdb.connect(database=duckdb_path, read_only=False)
con.execute('INSTALL excel;')
con.execute('LOAD excel;')
generate_report.duckdb_init(metadata_manuf_model_norm_path=manuf_model_norm_path, 
                            metadata_ppg_names_path=ppg_names_path,
                            metadata_site_mapping_path=site_mapping_path,
                            s4_soev_glob_path=s4_soev_glob_path,
                            ai2_soev_glob_path=ai2_soev_glob_path,
                            con=con)



generate_report.gen_xls_report(xls_output_path=output_report, con=con)
    
con.close()

print(f"Done - created: {output_report}")

