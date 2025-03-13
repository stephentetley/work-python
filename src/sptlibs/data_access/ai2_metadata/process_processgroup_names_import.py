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
import sptlibs.data_access.import_utils2 as import_utils2
from sptlibs.utils.sql_script_runner import SqlScriptRunner


def duckdb_import(*, xlsx_path: str, con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='process_processgroup_names_create_tables.sql')
    import_utils2.insert_into_by_name_xlsx(qualified_table_name='ai2_metadata.process_group_names',
                                           select_spec='"ProcessGroupAssetTypeDescription" AS process_group_name,',
                                           file_path=xlsx_path,
                                           sheet_name='process_group',
                                           con=con
                                           )
    import_utils2.insert_into_by_name_xlsx(qualified_table_name='ai2_metadata.process_names',
                                           file_path=xlsx_path,
                                           select_spec='"ProcessAssetTypeDescription" as process_name,',
                                           sheet_name='process',
                                           con=con
                                           )
