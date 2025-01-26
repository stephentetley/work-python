
import duckdb
from sptlibs.utils.xlsx_source import XlsxSource
import sptapps.equi_asset_translation.equi_asset_translation as equi_asset_translation

## (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
## (base) > python.exe .\scripts\temp_equi_translation.py

source_folder  = 'g:/work/2024/ai2_to_s4/mal12'
s4_classlists_source = 'g:/work/2024/asset_data_facts/s4_classlists/classlists_nov2024.duckdb'
ai2_equipment_attributes_source = XlsxSource('G:/work/2025/equi_translation/ai2_metadata/EquipmentAttributes.xlsx', 'AllData')    
duckdb_output_path  = 'g:/work/2025/equi_translation/mal12_new_equi_translation.duckdb'




con = duckdb.connect(database=duckdb_output_path, read_only=False)

equi_asset_translation.setup_equi_translation(con=con, 
                                              s4_classlists_db_source=s4_classlists_source,
                                              ai2_equipment_attributes_source=ai2_equipment_attributes_source)

# load ai2 exports into landing area...
equi_asset_translation.import_ai2_exports_to_ai2_landing(con=con, 
                                                         source_folder=source_folder,
                                                         glob_pattern='mal12-ai2*.xlsx')

equi_asset_translation.ai2_landing_data_to_ai2_eav(con=con)
equi_asset_translation.translate_ai2_eav_to_ai2_classrep(con=con)

con.close()
print(f'wrote {duckdb_output_path}')


