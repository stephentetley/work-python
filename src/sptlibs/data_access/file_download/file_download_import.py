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
from sptlibs.utils.sql_script_runner2 import SqlScriptRunner2
from sptlibs.data_access.file_download._file_download import _FileDownload

def duckdb_table_init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner2(__file__, con=con)
    runner.exec_sql_file(rel_file_path='s4_fd_raw_data_create_tables.sql')


def duckdb_import(*, path: str, con: duckdb.DuckDBPyConnection) -> None:
    fd = _FileDownload.from_download(path=path)
    if fd:
        fd.store_to_duckdb(con=con)


def duckdb_import_directory(*, source_dir: str, glob_pattern: str, con: duckdb.DuckDBPyConnection) -> None:
    globlist = glob.glob(glob_pattern, root_dir=source_dir, recursive=False)
    for file_name in globlist: 
        path = os.path.normpath(os.path.join(source_dir, file_name))
        print(path)
        try:
            duckdb_import(path=path, con=con)
        except Exception as exn:
            print(exn)
            continue
