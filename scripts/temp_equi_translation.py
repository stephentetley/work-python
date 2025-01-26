
import duckdb
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptapps.equi_translation.equi_translation_setup as equi_translation_setup

## (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
## (base) > python.exe .\scripts\temp_equi_translation.py

source_folder  = 'g:/work/2024/ai2_to_s4/mal12'
s4_classlists_source = 'g:/work/2024/asset_data_facts/s4_classlists/classlists_nov2024.duckdb'
ai2_equipment_attributes_source = XlsxSource('G:/work/2025/equi_translation/ai2_metadata/EquipmentAttributes.xlsx', 'AllData')    
duckdb_output_path  = 'g:/work/2025/equi_translation/mal12_new_equi_translation.duckdb'




con = duckdb.connect(database=duckdb_output_path, read_only=False)

equi_translation_setup.setup_equi_translation(con=con, 
                                              s4_classlists_db_source=s4_classlists_source,
                                              ai2_equipment_attributes_source=ai2_equipment_attributes_source)

runner = SqlScriptRunner()

# load ai2 exports into landing area...
equi_translation_setup.import_ai2_exports_to_ai2_landing(con=con, 
                                                         source_folder=source_folder,
                                                         glob_pattern='mal12-ai2*.xlsx')

equi_translation_setup.translate_ai2_eav_to_ai2_classrep(con=con)

# runner.exec_sql_file(file_rel_path='equi_translation/ai2_classrep_insert_into.sql', con=con)
# runner.exec_sql_generating_file(file_rel_path='ai2_equi_classrep/gen_ai2_equiclass_insert_into.sql', con=con)
con.close()
print(f'wrote {duckdb_output_path}')


