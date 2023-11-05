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
import glob
import sqlite3 as sqlite3
import pandas as pd
from typing import Callable
import sptlibs.import_utils as import_utils
import sptlibs.file_download.file_download_parser as file_download_parser

# TODO - store sorce file names in the duckdb

class GenSqlite:
    def __init__(self, *, output_directory: str) -> None:
        self.db_name = 'file_download_imports.sqlite3'
        self.output_dir = output_directory
        self.imports = []

    def add_file_download(self, *, path: str) -> None:
        self.imports.append((path))

    def add_file_downloads_in_directory(self, *, path: str, glob_pattern: str) -> None:
        globlist = glob.glob(glob_pattern, root_dir=path, recursive=False)
        for file_name in globlist: 
            self.imports.append(os.path.join(path, file_name))


    def gen_sqlite(self) -> str:
        sqlite_outpath = os.path.normpath(os.path.join(self.output_dir, self.db_name))
        tables = {}
        con = sqlite3.connect(sqlite_outpath)
        for path in self.imports:
            try:
                dfp = file_download_parser.parse_file_download(path)
                if dfp is None:
                    print(f'Parsing failed for {path}')
                else: 
                    table_name = import_utils.normalize_name('%s_%s' % (dfp['entity_type'], dfp['variant']))
                    if not table_name in tables:
                        drop_table_sql = f'DROP TABLE IF EXISTS {table_name};'
                        con.execute(drop_table_sql)
                        tables[table_name] = True
                    self.__gen_sqlite1(dfp, table_name=table_name, con=con, df_trafo=None)
            except Exception as exn:
                print(exn)
                continue
        con.close()
        print(f'{sqlite_outpath} created')
        return sqlite_outpath


    def __gen_sqlite1(self, dict, *, table_name: str, con: sqlite3.Connection, df_trafo: Callable[[pd.DataFrame], pd.DataFrame]) -> None:
        if dict is not None:
            df_raw = dict['dataframe']
            if df_trafo is not None:
                df_clean = df_trafo(df_raw)
            else:
                df_clean = df_raw
            df_renamed = import_utils.normalize_df_column_names(df_clean)
            df_renamed.to_sql(table_name, con)
            con.commit()
        else:
            print('__gen_sqlite1 - dict is None')

