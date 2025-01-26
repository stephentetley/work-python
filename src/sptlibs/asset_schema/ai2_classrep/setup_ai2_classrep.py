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
from sptlibs.utils.sql_script_runner2 import SqlScriptRunner2

def setup_ai2_classrep_tables(con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner2(__file__, con=con)
    runner.exec_sql_file(rel_file_path='setup_ai2_equi_classrep.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_ai2_equiclass_create_tables.sql')
