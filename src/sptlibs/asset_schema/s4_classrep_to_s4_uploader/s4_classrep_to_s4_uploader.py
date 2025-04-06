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
import sptlibs.data_access.s4_uploader.s4_uploader_export as s4_uploader_export

def translate_s4_classrep_to_s4_loader(*, con: duckdb.DuckDBPyConnection) -> None: 
    s4_uploader_export.duckdb_init(con=con)
    runner = SqlScriptRunner(__file__, con=con)   
    # insert data...
    runner.exec_sql_file(rel_file_path='equi_insert_into.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_equiclass_insert_into.sql')
    
