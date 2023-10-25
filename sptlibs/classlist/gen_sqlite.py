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
import sptlibs.classlist.classlist_parser as classlist_parser

    
class GenSqlite:
    def __init__(self, *, output_directory: str) -> None:
        self.db_name = 'classlists.sqlite3'
        self.output_dir = output_directory
        self.enums_table_name = 'classlists_enum_values'
        self.characteristics_table_name = 'classlists_characteristics'
        self.imports = []

    def add_classlist(self, path: str) -> None:
        self.imports.append(path)

    def gen_sqlite(self) -> str:
        sqlite_outpath = os.path.join(self.output_dir, self.db_name)
        chars_list = []
        enums_list = []
        for path in self.imports:
            try:
                dfp = classlist_parser.parse_classsfile(path)
                if dfp is None:
                    print(f'Parsing failed for {path}')
                else: 
                    chars_list.append(dfp['characteristics'])
                    enums_list.append(dfp['enum_values'])
            except Exception as exn:
                print(exn)
                continue
        con = sqlite3.connect(sqlite_outpath)
        chars_df = pd.concat(chars_list, ignore_index=True)
        enums_df = pd.concat(enums_list, ignore_index=True)
        self.__gen_sqlite1(chars_df, table_name=self.characteristics_table_name, con=con)
        self.__gen_sqlite1(enums_df, table_name=self.enums_table_name, con=con)
        con.close()
        print(f'{sqlite_outpath} created')
        return sqlite_outpath


    def __gen_sqlite1(self, df: pd.DataFrame, *, table_name: str, con: sqlite3.Connection) -> None:
        '''Note drops the table `table_name` before filling it'''
        if df is not None:
            con.execute(f'DROP TABLE IF EXISTS {table_name};')
            df.to_sql(name=table_name, con=con)
            con.commit()
        else:
            print(f'__gen_sqlite1 - dataframe is None for {table_name}')