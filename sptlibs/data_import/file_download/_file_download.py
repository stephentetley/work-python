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
import polars as pl
import sptlibs.data_import.import_utils as import_utils

class _FileDownload:
    def __init__(self, *, data_model: str=None, entity_type: str=None, 
                 variant: str=None, date: int=None, time: int=None,
                 payload: pl.DataFrame=None):
        self.entity_type = entity_type
        self.data_model = data_model
        self.variant = variant
        self.date = date
        self.time = time
        self.payload = payload

    @classmethod
    def from_download(cls, path: str):
        re_data_model = re.compile(r"\* Data Model: (?P<data_model>\w+)")
        re_entity_type = re.compile(r"\* Entity Type: (?P<entity_type>\w+)")
        re_variant = re.compile(r"\* Variant: (?P<variant>\w+)")
        re_date_time = re.compile(r"\* Date: (?P<date>\d+) / Time: (?P<time>\d+)")
        re_columns = re.compile(r"\*\w+")
        re_tab = re.compile(r"\t{1}")
        payload = False
        rows = []
        with open(path, 'r', encoding='utf-8-sig') as infile:
            for line in infile.readlines():
                if payload == False:
                    find_data_model = re_data_model.search(line)
                    find_entity_type = re_entity_type.search(line)
                    find_variant = re_variant.search(line)
                    find_date_time = re_date_time.search(line)
                    column_prefix = re_columns.search(line)
                    if find_data_model:
                        data_model = find_data_model.group('data_model')
                    if find_entity_type:
                        entity_type = find_entity_type.group('entity_type')
                    if find_variant:
                        variant = find_variant.group('variant')
                    if find_date_time:
                        date = int(find_date_time.group('date'))
                        time = int(find_date_time.group('time'))
                    if column_prefix:
                        payload = True
                        columns = re_tab.split(line)
                        _tidy_headers(columns)
                else:
                    row = re_tab.split(line)
                    if len(row) > 0: 
                        _tidy_row(row)
                        rows.append(row)
        df = pl.DataFrame(data=rows, schema=columns)
        df_renamed = import_utils.normalize_df_column_names(df)
        return cls(data_model=data_model, entity_type=entity_type, 
                   date=date, time=time,
                   variant=variant, payload=df)

    def store_to_duckdb(self, *, table_name: str, con: duckdb.DuckDBPyConnection) -> None:
        con.register(view_name='vw_df_file_download', python_object=self.payload)
        sql_stmt = f'CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM vw_df_file_download;'
        con.execute(sql_stmt)
        con.commit()

    def gen_std_table_name(self, *, schema_name: str) -> str: 
        table_name1 = import_utils.normalize_name(f'{self.entity_type}_{self.variant}')
        return f'{schema_name}.{table_name1}'


def _tidy_headers(headers: list[str]):
    first = headers[0]
    last = headers.pop()
    headers[0] = first[1:]
    headers.append(last.rstrip())

def _tidy_row(row: list[str]):
    last = row.pop()
    if last != '\n':
        row.append(last)
 
