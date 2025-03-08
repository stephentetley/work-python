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

def duckdb_init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner(__file__, con=con)
    # create tables, views...
    runner.exec_sql_file(rel_file_path='s4_class_rep_create_tables.sql')
    runner.exec_sql_file(rel_file_path='s4_class_rep_create_views.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_s4_class_rep_flocclass_tables.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_s4_class_rep_equiclass_tables.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_s4_class_rep_flocsummary_views.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_s4_class_rep_equisummary_views.sql')
    # insert data...
    runner.exec_sql_file(rel_file_path='s4_class_rep_insert_into.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_s4_class_rep_flocclass_insert_into.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_s4_class_rep_equiclass_insert_into.sql')
    
