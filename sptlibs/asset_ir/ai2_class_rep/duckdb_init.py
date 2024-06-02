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
from duckdb.typing import *
from utils.sql_script_runner import SqlScriptRunner
from sptlibs.utils.grid_ref import Osgb36

def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    con.create_function("udf_get_easting", udf_get_easting, [VARCHAR], INTEGER)
    con.create_function("udf_get_northing", udf_get_northing, [VARCHAR], INTEGER)
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='ai2_class_rep/ai2_class_rep_create_tables.sql', con=con)
    runner.exec_sql_file(file_rel_path='ai2_class_rep/ai2_class_rep_insert_into.sql', con=con)

def udf_get_easting(osgb: str) -> int:
    return Osgb36(osgb).to_east_north().easting

def udf_get_northing(osgb: str) -> int:
    return Osgb36(osgb).to_east_north().northing

