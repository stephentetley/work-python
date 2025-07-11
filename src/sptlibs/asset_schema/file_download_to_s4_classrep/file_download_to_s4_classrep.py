"""
Copyright 2024 Stephen Tetley

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

import duckdb
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptlibs.asset_schema.s4_classrep.setup_s4_classrep as setup_classrep

def translate_file_download_to_s4_classrep(*, 
                                           s4_classlists_db_path: str, 
                                           con: duckdb.DuckDBPyConnection) -> None: 
    setup_classrep.duckdb_init_s4_classrep(s4_classlists_db_path=s4_classlists_db_path, 
                                           equi_class_tables=None,
                                           floc_class_tables=None,
                                           con=con)
    runner = SqlScriptRunner(__file__, con=con)    
    # insert data...
    runner.exec_sql_file(rel_file_path='s4_classrep_insert_into.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_s4_classrep_flocclass_insert_into.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_s4_classrep_equiclass_insert_into.sql')
    
