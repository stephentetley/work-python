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

import os
import glob
import duckdb
from sptlibs.utils.sql_script_runner import SqlScriptRunner
from sptlibs.data_access.file_download._file_download import _FileDownload

def duckdb_init(*, xlsx_directory_glob: str, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner(__file__, con=con)
    runner.exec_sql_file(rel_file_path='aide_changes_create_tables.sql')
    runner.exec_sql_generating_file(rel_file_path='gen_aide_export_insert.sql', parameters={'globpath': xlsx_directory_glob})


def export_xlsx(*, xlsx_path: str, con: duckdb.DuckDBPyConnection) -> None: 
    query = f"""
        COPY (
            SELECT DISTINCT ON(asset_ref) * FROM soev_rawdata.vw_aide_changes
            ORDER BY common_name
        )
        TO '{xlsx_path}' WITH (FORMAT 'xlsx', HEADER true);
    """
    con.execute(query=query)



