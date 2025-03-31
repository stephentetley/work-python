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
import polars.selectors as cs
import sptlibs.data_access.import_utils as import_utils


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
        headers = _read_header(path)
        column_names = headers.get('columns')
        dtypes = dtypes={"column_1": pl.String}
        if headers.get('variant') == 'VALUAEQUI1': 
            dtypes = {"column_1": pl.String, "column_16": pl.Float64, "column_17": pl.Float64}
        if headers.get('variant') == 'VALUAFLOC1': 
            dtypes = {"column_1": pl.String, "column_16": pl.Float64, "column_17": pl.Float64}
        if column_names:
            body = pl.read_csv(path, new_columns = column_names, dtypes=dtypes, has_header=False, separator='\t', comment_prefix='*')
            body = body.drop(cs.contains("column_"))
            return cls(data_model=headers.get('data_model'), entity_type=headers.get('entity_type'), 
                       date=headers.get('date'), time=headers.get('time'),
                       variant=headers.get('variant'), payload=body)
        else:
            return None
        
    def store_to_duckdb(self, *, con: duckdb.DuckDBPyConnection) -> None:
        table_name = import_utils.normalize_name(f'{self.entity_type}_{self.variant}')
        view_name = import_utils.normalize_name(f'vw_{self.entity_type}')
        con.register(view_name=view_name, python_object=self.payload)
        sql_stmt = f'CREATE OR REPLACE TABLE fd_landing.{table_name} AS SELECT * FROM {view_name};'
        con.execute(sql_stmt)
        con.commit()


def _read_header(path) -> dict:
    headers = {}
    re_data_model = re.compile(r"\* Data Model: (?P<data_model>\w+)")
    re_entity_type = re.compile(r"\* Entity Type: (?P<entity_type>\w+)")
    re_variant = re.compile(r"\* Variant: (?P<variant>\w+)")
    re_date_time = re.compile(r"\* Date: (?P<date>\d+) / Time: (?P<time>\d+)")
    re_columns = re.compile(r"\*(EQUI|FUNCLOC)\t")
    with open(path, 'r', encoding='utf-8-sig') as infile:
        for line in infile.readlines():
            find_data_model = re_data_model.search(line)
            find_entity_type = re_entity_type.search(line)
            find_variant = re_variant.search(line)
            find_date_time = re_date_time.search(line)
            find_columns = re_columns.search(line)
            if find_data_model:
                headers['data_model'] = find_data_model.group('data_model')
            if find_entity_type:
                headers['entity_type'] = find_entity_type.group('entity_type')
            if find_variant:
                headers['variant'] = find_variant.group('variant')
            if find_date_time:
                headers['date'] = int(find_date_time.group('date'))
                headers['time'] = int(find_date_time.group('time'))
            if find_columns:
                headers['columns'] = _get_headings(line)
    return headers

def _get_headings(headers: str) -> list[str]:
    def clean1(s: str) -> str : 
        if s.startswith("*"): 
            return s[1:].strip().lower()
        else: 
            return s.strip().lower()
    return [clean1(s) for s in headers.split()]


