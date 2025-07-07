# temp_telem_asset_replace.py

# > cd ~/_working/coding/work/work-python/
# > conda activate /home/stephen/miniconda3
# > conda activate develop-env
# > export PYTHONPATH="$HOME/_working/coding/work/work-python/src"
# > python scripts/temp_telem_asset_replace.py

import os
import datetime
import duckdb

from sptlibs.utils.xlsx_source import XlsxSource    
import sptapps.telemetry_asset_replace.setup_db as setup_db
import sptlibs.asset_schema.udfs.setup_sql_udfs as setup_sql_udfs
import sptlibs.data_access.excel_uploader.excel_uploader_equi_create as excel_uploader_equi_create
import sptlibs.data_access.excel_uploader.excel_uploader_equi_change as excel_uploader_equi_change
import sptlibs.data_access.rts_outstations.rts_outstations_import as rts_outstations_import
import sptlibs.data_access.ih06_ih08.ih08_import as ih08_import
import sptlibs.data_access.ai2_export.ai2_export_import as ai2_export_import
import sptlibs.asset_schema.s4_classrep.setup_s4_classrep as setup_s4_classrep

sheet_name = 'A1'

duckdb_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/telem_asset_replace_jun25_db.duckdb')    
worklist_path = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jul_07th/asset_replacement_worklist_20250707.xlsx')
uploader_create_template = os.path.expanduser('~/_working/work/2025/excel_uploader/templates/AIW_Equi_Creation_Template_V1.0.xlsx')
uploader_change_template = os.path.expanduser('~/_working/work/2025/excel_uploader/templates/AIW_Equi_Change_Template V1.0.xlsx')
rts_source_path = os.path.expanduser('~/_working/work/2025/rts/rts_outstations_report_20250625.tsv')
ih08_source = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/ih08_s4_prod_netwtl.xlsx')
ai2_equi_source = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/ai2_equi_outstation_export.xlsx')
ai2_floc_source = os.path.expanduser('~/_working/work/2025/great_telemetry_reconcile/jun_25th/ai2_parent1_export.xlsx')
s4_classlists_db = os.path.expanduser('~/_working/work/2025/asset_data_facts/s4_classlists/s4_classlists_apr2025.duckdb')
ai2_equipment_attributes_source = XlsxSource(os.path.expanduser('~/_working/work/2025/asset_data_facts/ai2_metadata/AI2AssetTypeAttributes20250123.xlsx'), 'AssetTypesAttributes')
ai2_equipment_attribute_sets = XlsxSource(os.path.expanduser('~/_working/work/2025/asset_data_facts/ai2_metadata/equipment_attribute_sets.xlsx'), 'Sheet1')
output_create_xlsx = os.path.expanduser(f'~/_working/work/2025/great_telemetry_reconcile/jul_07th/{sheet_name}_create_equi.xlsx')
output_change_xlsx = os.path.expanduser(f'~/_working/work/2025/great_telemetry_reconcile/jul_07th/{sheet_name}_update_equi.xlsx')


if os.path.exists(duckdb_path):
    os.remove(duckdb_path)

if os.path.exists(output_create_xlsx):
    os.remove(output_create_xlsx)

if os.path.exists(output_change_xlsx):
    os.remove(output_change_xlsx)

con = duckdb.connect(database=duckdb_path, read_only=False)
setup_sql_udfs.setup_udfx_macros(con=con)
setup_db.init_db(worklist_path=worklist_path,
                 sheet_name=sheet_name,
                 con=con)
rts_outstations_import.duckdb_import(rts_source_path, con=con)
ih08_import.duckdb_init(con=con)
ih08_import.duckdb_import_files(file_paths=[ih08_source],
                                con=con)
ai2_export_import.duckdb_init(con=con)
ai2_export_import.duckdb_import_landing_files(sources=[ai2_equi_source, ai2_floc_source],
                                              con=con)

setup_s4_classrep.duckdb_init_s4_classrep(s4_classlists_db_path=s4_classlists_db, 
                                          equi_class_tables=['NETWTL'], 
                                          floc_class_tables=[],
                                          con=con)


datestr = datetime.date.today().strftime('%d.%m.%y')

## create
excel_uploader_equi_create.duckdb_init(con=con)
excel_uploader_equi_change.duckdb_init(con=con)

create_header = f"Telemetry bulk CREATE upload {sheet_name} (??) {datestr}"
change_header = f"Telemetry bulk CHANGE upload {sheet_name} (??) {datestr}"
notes1 = f"Telemetry bulk upload created by telem_asset_replace, batch {sheet_name.lower()}"
notes2 = f"Update file created on {datetime.date.today().strftime('%d.%m.%Y')}"
setup_db.fill_db(create_header=create_header,
                 create_notes=[notes1, notes2],
                 change_header=change_header,
                 change_notes=[notes1, notes2],
                 con=con)
excel_uploader_equi_create.write_excel_upload(upload_template_path=uploader_create_template,
                                              dest=output_create_xlsx,
                                              con=con)


excel_uploader_equi_change.write_excel_upload(upload_template_path=uploader_change_template,
                                              dest=output_change_xlsx,
                                              con=con)

con.close()

print(f"Done - added raw data to: {duckdb_path}")
print(f"Created - {output_create_xlsx}")
print(f"Created - {output_change_xlsx}")



