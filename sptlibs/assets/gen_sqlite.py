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
import sptlibs.import_utils as import_utils
from sptlibs.xlsx_source import XlsxSource

    
class GenSqlite:
    def __init__(self, *, output_directory: str) -> None:
        self.db_name = 'assets_imports.sqlite3'
        self.output_dir = output_directory
        self.xlsx_imports = []

    def add_xlsx_source(self, xlsx: XlsxSource, table_name: str, df_trafo: Callable[[pd.DataFrame], pd.DataFrame]) -> None:
        self.xlsx_imports.append((xlsx, table_name, df_trafo))



    def gen_sqlite(self) -> str:
        sqlite_outpath = os.path.normpath(os.path.join(self.output_dir, self.db_name))
        con = sqlite3.connect(sqlite_outpath)
        for (src, table_name, df_trafo) in self.xlsx_imports:
            import_utils.sqlite_import_sheet(src, table_name=table_name, if_exists='replace', con=con, df_trafo=df_trafo)
        con.close()
        print(f'{sqlite_outpath} created')
        return sqlite_outpath

