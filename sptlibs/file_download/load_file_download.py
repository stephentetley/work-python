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
import duckdb
import pandas as pd
import sptlibs.import_utils as import_utils


def load_file_download(*, path: str, con: duckdb.DuckDBPyConnection) -> None:
    re_entity_type = re.compile(r"\* Entity Type: (?P<entity_type>\w+)")
    re_variant = re.compile(r"\* Variant: (?P<variant>\w+)")
    re_columns = re.compile(r"\*\w+")
    re_tab = re.compile(r"\t{1}")
    payload = False
    rows = []
    with open(path, 'r', encoding='utf-8-sig') as infile:
        for line in infile.readlines():
            if payload == False:
                find_entity_type = re_entity_type.search(line)
                find_variant = re_variant.search(line)
                column_prefix = re_columns.search(line)
                if find_entity_type:
                    entity_type = find_entity_type.group('entity_type')
                if find_variant:
                    variant = find_variant.group('variant')
                if column_prefix:
                    payload = True
                    columns = re_tab.split(line)
                    _tidy_headers(columns)
            else:
                row = re_tab.split(line)
                if len(row) > 0: 
                    _tidy_row(row)
                    rows.append(row)
    table_name1 = import_utils.normalize_name(f'{entity_type}_{variant}')
    qualified_table_name = f's4_fd_raw_data.{table_name1}'

    drop_table_sql = f'DROP TABLE IF EXISTS {qualified_table_name};'
    con.execute(drop_table_sql)
    df = pd.DataFrame(rows, columns = columns)
    df_renamed = import_utils.normalize_df_column_names(df)
    vw_df = f'vw_df_{table_name1}'
    con.register(view_name=vw_df, python_object=df_renamed)
    sql_stmt = f'CREATE TABLE {qualified_table_name} AS SELECT * FROM {vw_df};'
    con.execute(sql_stmt)
    con.commit()
   
def _tidy_headers(headers: list[str]):
    first = headers[0]
    last = headers.pop()
    headers[0] = first[1:]
    headers.append(last.rstrip())

def _tidy_row(row: list[str]):
    last = row.pop()
    if last != '\n':
        row.append(last)
 
