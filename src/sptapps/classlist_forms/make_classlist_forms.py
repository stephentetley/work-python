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
import pandas
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def get_class_enums(*, class_name: str, con: duckdb.DuckDBPyConnection) -> pandas.DataFrame:
    runner = SqlScriptRunner(__file__, con=con)
    setvar_stmt = f"SET VARIABLE equiclass_name = '{class_name.upper()}';"
    con.execute(setvar_stmt)
    rel = runner.eval_sql_file(rel_file_path='get_class_enums.sql')
    return rel.df()
    
def get_class_enums_dimensions(*, class_name: str, con: duckdb.DuckDBPyConnection) -> pandas.DataFrame:
    runner = SqlScriptRunner(__file__, con=con)
    setvar_stmt = f"SET VARIABLE equiclass_name = '{class_name.upper()}';"
    con.execute(setvar_stmt)
    rel = runner.eval_sql_file(rel_file_path='get_class_enums_dimensions.sql')
    return rel.df()

def get_char_form_data(*, class_name: str, con: duckdb.DuckDBPyConnection) -> pandas.DataFrame:
    runner = SqlScriptRunner(__file__, con=con)
    setvar_stmt = f"SET VARIABLE equiclass_name = '{class_name.upper()}';"
    con.execute(setvar_stmt)
    rel = runner.eval_sql_file(rel_file_path='get_char_form_data.sql')
    return rel.df()
