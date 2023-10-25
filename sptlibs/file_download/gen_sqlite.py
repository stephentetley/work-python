"""
Copyright 2023 Stephen Tetley

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
import sqlite3 as sqlite3
import pandas as pd
from typing import Callable
import sptlibs.file_download.file_download_parser as file_download_parser

    
class GenSqlite:
    def __init__(self, *, output_directory: str) -> None:
        self.db_name = 'file_download_imports.sqlite3'
        self.output_dir = output_directory
        self.imports = []

    def add_file_download(self, path: str, *, table_name: str, df_trafo: Callable[[pd.DataFrame], pd.DataFrame]) -> None:
        self.imports.append((path, table_name, df_trafo))



    def gen_sqlite(self) -> str:
        sqlite_outpath = os.path.join(self.output_dir, self.db_name)
        con = sqlite3.connect(sqlite_outpath)
        for (path, table_name, df_trafo) in self.imports:
            try:
                dfp = file_download_parser.parse_file_download(path)
                if dfp is None:
                    print(f'Parsing failed for {path}')
                else: 
                    file_download_parser.gen_sqlite(dfp, table_name=table_name, con=con, df_trafo=df_trafo)
            except Exception as exn:
                print(exn)
                continue
        con.close()
        print(f'{sqlite_outpath} created')
        return sqlite_outpath

