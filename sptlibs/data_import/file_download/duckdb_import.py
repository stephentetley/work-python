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
from utils.sql_script_runner import SqlScriptRunner
from sptlibs.data_import.file_download._file_download import _FileDownload

def init_s4_fd_raw_data_tables(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='s4_fd_raw_data/s4_fd_raw_data_create_tables.sql', con=con)


def store_download_file(*, path: str, con: duckdb.DuckDBPyConnection) -> None:
    fd = _FileDownload.from_download(path=path)
    if fd:
        fd.store_to_duckdb(con=con)


def store_download_files(*, source_dir: str, glob_pattern: str, con: duckdb.DuckDBPyConnection) -> None:
    globlist = glob.glob(glob_pattern, root_dir=source_dir, recursive=False)
    for file_name in globlist: 
        path = os.path.normpath(os.path.join(source_dir, file_name))
        print(path)
        try:
            store_download_file(path=path, con=con)
        except Exception as exn:
            print(exn)
            continue
