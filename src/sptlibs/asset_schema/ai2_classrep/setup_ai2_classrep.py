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
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptlibs.asset_schema.ai2_classrep.equipment_attributes_import as equipment_attributes_import

def duckdb_init(*, 
                equipment_attributes_source: XlsxSource,
                attribute_sets_source: XlsxSource,
                con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner(__file__, con=con)
    equipment_attributes_import.duckdb_import(equipment_attributes_source=equipment_attributes_source,
                                              attribute_sets_source=attribute_sets_source,
                                              con=con)
    runner.exec_sql_file(rel_file_path='setup_ai2_equi_classrep.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_ai2_equiclass_create_tables.sql')
