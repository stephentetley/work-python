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

import duckdb
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def setup_udfx_macros(*, con: duckdb.DuckDBPyConnection) -> None: 
    con.execute("CREATE SCHEMA IF NOT EXISTS udfx;")
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='east_north_create_macros.sql')
    runner.exec_sql_file(rel_file_path='floc_macros.sql')
    runner.exec_sql_file(rel_file_path='format_macros.sql')
    runner.exec_sql_file(rel_file_path='name_creation.sql')
    runner.exec_sql_file(rel_file_path='translation_macros.sql')
    
