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

import pathlib
import duckdb
import sptlibs.asset_schema.ai2_eav.setup_ai2_eav as setup_ai2_eav
import sptlibs.asset_schema.ai2_classrep.setup_ai2_classrep as setup_ai2_classrep
import sptlibs.asset_schema.ai2_eav_to_classrep.ai2_eav_to_ai2_classrep as ai2_eav_to_ai2_classrep
import sptlibs.asset_schema.s4_classrep.setup_s4_classrep as setup_s4_classrep
import sptlibs.asset_schema.udfs.setup_sql_udfs as setup_sql_udfs
import sptlibs.data_access.s4_classlists.s4_classlists_import as s4_classlists_import
import sptlibs.data_access.import_utils as import_utils
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptapps.equi_asset_translation.ai2_metadata_import as ai2_metadata_import



                           

def setup_equi_translation(*, con: duckdb.DuckDBPyConnection,
                           s4_classlists_db_source: str, 
                           ai2_equipment_attributes_source: XlsxSource, 
                           ai2_equipment_attribute_sets: XlsxSource) -> None:
    s4_classlists_import.copy_classlists_tables(classlists_source_db_path=s4_classlists_db_source, 
                                            setup_tables=True, 
                                            dest_con=con)
    ai2_metadata_import.duckdb_import(equipment_attributes_source=ai2_equipment_attributes_source,
                                      attribute_sets_source=ai2_equipment_attribute_sets,
                                      con=con)
    setup_ai2_eav.setup_ai2_eav_tables(con=con)    
    setup_ai2_classrep.setup_ai2_classrep_tables(con=con)
    setup_s4_classrep.duckdb_init(gen_flocclasses=False, con=con)
    setup_sql_udfs.setup_macros(con=con)

# load ai2 exports into landing area...
def import_ai2_exports_to_ai2_landing(*, con: duckdb.DuckDBPyConnection,
                                      source_folder: str,
                                      glob_pattern: str ='*.xlsx',
                                      sheet_name: str='Sheet1') -> int:
    con.execute('CREATE SCHEMA IF NOT EXISTS ai2_landing;')
    sources = import_utils.get_excel_sources_from_folder(source_folder=source_folder, 
                                                         glob_pattern=glob_pattern,
                                                         sheet_name=sheet_name)
    added = 0
    for source in sources:
        table_name_pre = pathlib.Path(source.path).stem
        if not table_name_pre.startswith('~$'):
            table_name = import_utils.normalize_name(table_name_pre)
            import_utils.duckdb_import_sheet(source=source, 
                                             qualified_table_name=f'ai2_landing.{table_name}', 
                                             con=con, 
                                             df_trafo=None)
            added = added + 1
    return added


def ai2_landing_data_to_ai2_eav(*, con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='ai2_landing/setup_ai2_landing_data_to_ai2_eav_macros.sql')
    runner.exec_sql_generating_file(rel_file_path='ai2_landing/gen_ai2_eav_equipment_masterdata_insert_into.sql')
    runner.exec_sql_generating_file(rel_file_path='ai2_landing/gen_ai2_eav_equipment_eav_insert_into.sql')


def translate_ai2_eav_to_ai2_classrep(*, con: duckdb.DuckDBPyConnection) -> None:
    ai2_eav_to_ai2_classrep.translate_ai2_eav_to_ai2_classrep(con=con)

    
def translate_ai2_classrep_to_s4_classrep(*, con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='ai2_classrep_to_s4_classrep/setup_equi_asset_translation.sql')
    runner.exec_sql_file(rel_file_path='ai2_classrep_to_s4_classrep/translate_equiclass_a_to_c.sql')
    runner.exec_sql_file(rel_file_path='ai2_classrep_to_s4_classrep/translate_equiclass_d_to_f.sql')
    runner.exec_sql_file(rel_file_path='ai2_classrep_to_s4_classrep/translate_equiclass_g_to_k.sql')
    runner.exec_sql_file(rel_file_path='ai2_classrep_to_s4_classrep/translate_equiclass_l_to_o.sql')
    runner.exec_sql_file(rel_file_path='ai2_classrep_to_s4_classrep/translate_equiclass_p_to_t.sql')
    runner.exec_sql_file(rel_file_path='ai2_classrep_to_s4_classrep/translate_equiclass_u_to_z.sql')
    runner.exec_sql_generating_file(rel_file_path='ai2_classrep_to_s4_classrep/gen_tt_equipment_classtypes_insert_into.sql')
    runner.exec_sql_file(rel_file_path='ai2_classrep_to_s4_classrep/s4_classrep_equi_masterdata_insert_into.sql')

