import pathlib
import duckdb
import sptlibs.data_access.import_utils as import_utils
import sptlibs.data_access.s4_classlists.s4_classlists_import as s4_classlists_import
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.utils.sql_script_runner import SqlScriptRunner

## (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'
## (base) > python.exe .\scripts\temp_equi_translation.py

source_folder  = 'g:/work/2024/ai2_to_s4/mal12'
s4_classlists_source = 'g:/work/2024/asset_data_facts/s4_classlists/classlists_nov2024.duckdb'
ai2_equipment_attributes_source = XlsxSource('G:/work/2025/equi_translation/ai2_metadata/EquipmentAttributes.xlsx', 'AllData')    
duckdb_output_path  = 'g:/work/2025/equi_translation/mal12_new_equi_translation.duckdb'

# TODO don't load as separate tables

sources = import_utils.get_excel_sources_from_folder(source_folder=source_folder, 
                                                     glob_pattern='mal12-ai2*.xlsx',
                                                     sheet_name='Sheet1')

con = duckdb.connect(database=duckdb_output_path, read_only=False)

s4_classlists_import.copy_classlists_tables(classlists_source_db_path=s4_classlists_source, 
                                            setup_tables=True, 
                                            dest_con=con)

con.execute("CREATE SCHEMA IF NOT EXISTS ai2_metadata;")
import_utils.duckdb_import_sheet(source=ai2_equipment_attributes_source, 
                                 qualified_table_name=f'ai2_metadata.equipment_attributes', 
                                 con=con, 
                                 df_trafo=None)

runner = SqlScriptRunner()
# This should be encapsulated in a module...
runner.exec_sql_file(file_rel_path='ai2_equi_classrep/setup_ai2_equi_classrep.sql', con=con)
runner.exec_sql_generating_file(file_rel_path='ai2_equi_classrep/gen_ai2_equiclass_tables.sql', con=con)

runner.exec_sql_file(file_rel_path='equi_translation/setup_equi_translation.sql', con=con)

for source in sources:
    table_name = import_utils.normalize_name(pathlib.Path(source.path).stem)
    import_utils.duckdb_import_sheet(source=source, 
                                     qualified_table_name=f'ai2_landing.{table_name}', 
                                     con=con, 
                                     df_trafo=None)


runner.exec_sql_generating_file(file_rel_path='equi_translation/gen_table_equipment_masterdata.sql', con=con)
runner.exec_sql_generating_file(file_rel_path='equi_translation/gen_table_equipment_eav.sql', con=con)

runner.exec_sql_file(file_rel_path='equi_translation/ai2_classrep_insert_into.sql', con=con)

con.close()
print(f'wrote {duckdb_output_path}')


