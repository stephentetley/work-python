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
import sptlibs.import_utils as import_utils
from sptlibs.xlsx_source import XlsxSource
import sptlibs.ih06_ih08.transform_xlsx as transform_xlsx

    
class GenSqlite:
    def __init__(self, *, output_directory: str) -> None:
        self.db_name = 'ih06ih08.sqlite3'
        self.output_dir = output_directory
        self.xlsx_imports = []

    def set_db_name(self, *, db_name: str) -> None:
        '''Just the name, not the path.'''
        self.db_name = db_name

    def add_ih06_export(self, src: XlsxSource) -> None:
        self.xlsx_imports.append(src)

    def add_ih08_export(self, src: XlsxSource) -> None:
        self.xlsx_imports.append(src)


    def gen_sqlite(self) -> str:
        '''Deletes the existing database!'''
        sqlite_outpath = os.path.normpath(os.path.join(self.output_dir, self.db_name))
        try:
            os.remove(sqlite_outpath)
        except OSError:
            pass
        con = sqlite3.connect(sqlite_outpath)
        for src in self.xlsx_imports:
            tables = transform_xlsx.parse_ih08(xlsx_src=src)
            for table in tables:
                df = table['data_frame']
                table_name = table['table_name']
                df.to_sql(name=table_name, if_exists='append', con=con)
        con.close()
        print(f'{sqlite_outpath} created')
        return sqlite_outpath

