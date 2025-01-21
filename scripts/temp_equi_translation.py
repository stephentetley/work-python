import pathlib
import duckdb
import sptlibs.data_access.import_utils as import_utils
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.utils.sql_script_runner import SqlScriptRunner

## (base) > $env:PYTHONPATH='E:\coding\work\work-python\src'

source_folder  = 'g:/work/2024/ai2_to_s4/mal12'
duckdb_output_path  = 'g:/work/2025/equi_translation/mal12_new_equi_translation.duckdb'

# TODO don't load as separate tables

sources = import_utils.get_excel_sources_from_folder(source_folder=source_folder, 
                                                     glob_pattern='mal12-ai2*.xlsx',
                                                     sheet_name='Sheet1')

con = duckdb.connect(database=duckdb_output_path, read_only=False)


runner = SqlScriptRunner()
runner.exec_sql_file(file_rel_path='equi_translation/setup_equi_translation.sql', con=con)

for source in sources:
    table_name = import_utils.normalize_name(pathlib.Path(source.path).stem)
    import_utils.duckdb_import_sheet(source=source, 
                                     qualified_table_name=f'ai2_landing.{table_name}', 
                                     con=con, 
                                     df_trafo=None)
equipment_attributes_src = XlsxSource('G:/work/2025/equi_translation/ai2_metadata/EquipmentAttributes.xlsx', 'AllData')    
import_utils.duckdb_import_sheet(source=equipment_attributes_src, 
                                qualified_table_name=f'ai2_metadata.equipment_attributes', 
                                     con=con, 
                                     df_trafo=None)

runner.exec_sql_generating_file(file_rel_path='equi_translation/gen_table_equipment_masterdata.sql', con=con)
runner.exec_sql_generating_file(file_rel_path='equi_translation/gen_table_equipment_eav.sql', con=con)

con.close()
print(f'wrote {duckdb_output_path}')


