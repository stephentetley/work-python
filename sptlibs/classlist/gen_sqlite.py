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
        self.enums_table_name = 'classlist_enum_values'
        self.characteristics_table_name = 'classlist_characteristics'
        self.classlist_dicts = []

    def add_floc_classlist(self, path: str) -> None:
        try:
            df1 = classlist_parser.parse_floc_classfile(path)
            if df1 is None:
                print(f'Parsing failed for {path}')
            else: 
                self.classlist_dicts.append(df1)
        except Exception as exn:
            print(exn)

    def add_equi_classlist(self, path: str) -> None:
        try:
            df1 = classlist_parser.parse_equi_classfile(path)
            if df1 is None:
                print(f'Parsing failed for {path}')
            else: 
                self.classlist_dicts.append(df1)
        except Exception as exn:
            print(exn)

    def gen_sqlite(self) -> str:
        sqlite_outpath = os.path.normpath(os.path.join(self.output_dir, self.db_name))
        chars_list = []
        enums_list = []
        for dict1 in self.classlist_dicts:
            chars_list.append(dict1['characteristics'])
            enums_list.append(dict1['enum_values'])
        con = sqlite3.connect(sqlite_outpath)
        if chars_list:
            chars_df = pd.concat(chars_list, ignore_index=True)
            self.__gen_sqlite1(chars_df, table_name=self.characteristics_table_name, con=con)
        if enums_list:
            enums_df = pd.concat(enums_list, ignore_index=True)
            self.__gen_sqlite1(enums_df, table_name=self.enums_table_name, con=con)
        con.close()
        print(f'{sqlite_outpath} created')
        return sqlite_outpath


    def __gen_sqlite1(self, df: pd.DataFrame, *, table_name: str, con: sqlite3.Connection) -> None:
        '''Note drops the table `table_name` before filling it'''
        if df is not None:
            df.to_sql(name=table_name, if_exists='replace', con=con)
            con.commit()
        else:
            print(f'__gen_sqlite1 - dataframe is None for {table_name}')