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



def duckdb_import(rts_report_csv: str, *, con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='rts_outstations_create_tables.sql')
    runner.exec_sql_file(rel_file_path='rts_outstations_insert_into.sql', parameters={'csv_file_path' : rts_report_csv})


