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
import tempfile
import duckdb
import sptlibs.import_utils as import_utils
from sptlibs.xlsx_source import XlsxSource


class GenDuckdb:
    def __init__(self) -> None:
        self.db_name = 'ztables.duckdb'
        self.output_directory = tempfile.gettempdir()
        self.xlsx_imports = []

    def set_output_directory(self, *, output_directory: str) -> None: 
        self.output_directory = output_directory

    def add_xlsx_source(self, xlsx: XlsxSource, table_name: str) -> None:
        self.xlsx_imports.append((xlsx, table_name))

    def gen_duckdb(self) -> str:
        '''pandas differentiates columns with the same name for us'''
        duckdb_outpath = os.path.normpath(os.path.join(self.output_directory, self.db_name))
        con = duckdb.connect(database=duckdb_outpath)
        for (src, table_name) in self.xlsx_imports:
            import_utils.duckdb_import_sheet(src, table_name=table_name, con=con, df_trafo=None, if_exists='replace')
        con.close()
        print(f'{duckdb_outpath} created')
        return duckdb_outpath

