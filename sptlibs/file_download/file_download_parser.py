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

import re
import sqlite3 as sqlite3
import pandas as pd
from typing import Callable
import sptlibs.import_utils as import_utils

    

# def __init__(self, path: str) -> None:
#     ans = self.__read_file_download(path)
#     if ans is not None:
#         self.entity_type = ans['entity_type']
#         self.dataframe = pd.DataFrame(ans['rows'], columns=ans['columns'])
#     else:
#         self.entity_type = "Invalid"
#         self.dataframe = None

    
def _tidy_headers(headers: list[str]):
    first = headers[0]
    last = headers.pop()
    headers[0] = first[1:]
    headers.append(last.rstrip())

def _tidy_row(row: list[str]):
    last = row.pop()
    if last != '\n':
        row.append(last)


def parse_file_download(path: str) -> dict:
    try: 
        re_entity_type = re.compile(r"\* Entity Type: (?P<entity_type>\w+)")
        re_columns = re.compile(r"\*\w+")
        re_tab = re.compile(r"\t{1}")
        payload = False
        rows = []
        with open(path, 'r') as infile:
            for line in infile.readlines():
                if payload == False:
                    find_entity_type = re_entity_type.search(line)
                    column_prefix = re_columns.search(line)
                    if find_entity_type:
                        entity_type = find_entity_type.group('entity_type')
                    if column_prefix:
                        payload = True
                        columns = re_tab.split(line)
                        _tidy_headers(columns)
                else:
                    row = re_tab.split(line)
                    if len(row) > 0: 
                        _tidy_row(row)
                        rows.append(row)
        ans = {}
        ans['entity_type'] = entity_type
        ans['dataframe'] = pd.DataFrame(rows, columns = columns)
        return ans
    except Exception:
        return None

def gen_sqlite(dict, *, table_name: str, con: sqlite3.Connection, df_trafo: Callable[[pd.DataFrame], pd.DataFrame]) -> None:
    '''Note drops the table `table_name` before filling it'''
    if dict is not None:
        df_raw = dict['dataframe']
        if df_trafo is not None:
            df_clean = df_trafo(df_raw)
        else:
            df_clean = df_raw
        df_renamed = import_utils.normalize_df_column_names(df_clean)
        con.execute(f'DROP TABLE IF EXISTS {table_name};')
        df_renamed.to_sql(table_name, con)
        con.commit()
    else:
        print('gen_sqlite - dict is None')
        