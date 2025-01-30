
import duckdb
from sptlibs.utils.xlsx_source import XlsxSource
import sptapps.equi_asset_translation.equi_asset_translation as equi_asset_translation

## (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
## (base) > python.exe .\scripts\temp_equi_translation.py

s4_classlists_source = 'g:/work/2024/asset_data_facts/s4_classlists/classlists_nov2024.duckdb'
ai2_equipment_attributes_source = XlsxSource('G:/work/2025/equi_translation/ai2_metadata/AI2AssetTypeAttributes20250123.xlsx', 'AssetTypesAttributes')
ai2_equipment_attribute_sets = XlsxSource('G:/work/2025/equi_translation/ai2_metadata/equipment_attribute_sets.xlsx', 'Sheet1')

source_folder  = 'g:/work/2024/ai2_to_s4/multi'
duckdb_output_path  = 'g:/work/2025/equi_translation/multi_new_equi_translation.duckdb'




con = duckdb.connect(database=duckdb_output_path, read_only=False)

equi_asset_translation.setup_equi_translation(con=con, 
                                              s4_classlists_db_source=s4_classlists_source,
                                              ai2_equipment_attributes_source=ai2_equipment_attributes_source, 
                                              ai2_equipment_attribute_sets=ai2_equipment_attribute_sets)

# load ai2 exports into landing area...

added = equi_asset_translation.import_ai2_exports_to_ai2_landing(con=con,
                                                                 source_folder=source_folder,
                                                                 glob_pattern='*.xlsx')

if added > 0:
    print(f'{added} exports added')
    equi_asset_translation.ai2_landing_data_to_ai2_eav(con=con)
    equi_asset_translation.translate_ai2_eav_to_ai2_classrep(con=con)
    equi_asset_translation.translate_ai2_classrep_to_s4_classrep(con=con)
else:
    print('No exports added')

con.close()
print(f'wrote {duckdb_output_path}')


