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
import sptlibs.data_access.s4_ztables.s4_ztables_import as s4_ztables_import
import sptlibs.data_access.excel_table.excel_table_import as excel_table_import
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptapps.floc_builder.generate_flocs as generate_flocs

def duckdb_init(*, duckdb_path: str, 
                worklist_path: str, 
                ih06_path: str, 
                ztable_source_db: str) -> None: 
    generate_flocs.generate_flocs(duckdb_path=duckdb_path,
                                  worklist_path=worklist_path,
                                  ih06_path=ih06_path,
                                  ztable_source_db=ztable_source_db)
