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
import polars as pl
import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.polars_import_utils as polars_import_utils
from sptlibs.ih06_ih08.column_range import ColumnRange

def load_ih06(*, xlsx_src: XlsxSource, con: duckdb.DuckDBPyConnection) -> None:
    config = {}
    config['range_name'] = 'floc_masterdata'
    config['qualified_table_name'] = 's4_ihx_raw_data.floc_masterdata'
    config['df_view_name'] = 'vw_df_floc'
    config['name_prefix'] = 'valuafloc'
    _load_ih_file(config=config, xlsx_src=xlsx_src, con=con)

def load_ih08(*, xlsx_src: XlsxSource, con: duckdb.DuckDBPyConnection) -> None:
    config = {}
    config['range_name'] = 'equi_masterdata'
    config['qualified_table_name'] = 's4_ihx_raw_data.equi_masterdata'
    config['df_view_name'] = 'vw_df_equi'
    config['name_prefix'] = 'valuaequi'
    _load_ih_file(config=config, xlsx_src=xlsx_src, con=con)


def _load_ih_file(*, config: dict, xlsx_src: XlsxSource, con: duckdb.DuckDBPyConnection) -> None:
    df = pl.read_excel(source=xlsx_src.path, sheet_name=xlsx_src.sheet, engine='xlsx2csv', read_csv_options = {'ignore_errors': True, 'null_values': ['NULL', 'Null', 'null']})
    re_class_start = re.compile(r"Class (?P<class_name>[\w_]+) is assigned")
    ranges = []
    # start at column 1, dropping column 0 `selected line`
    range1 = ColumnRange(range_name=config['range_name'], start=1)
    for (ix, col) in enumerate(df.columns):
        find_class_start = re_class_start.search(col)
        if find_class_start:
            ranges.append(range1)
            class_name = find_class_start.group('class_name')
            range1 = ColumnRange(range_name=class_name, start=ix)
        else:
            range1.range_end = ix
        print(ix, col)
    # add pending range
    ranges.append(range1)
    # load the data...
    _load_masterdata(qualified_table_name=config['qualified_table_name'], temp_view_name=config['df_view_name'], data_frame=df, column_range=ranges[0], con=con)
    for crange in ranges[1:]:
        _load_values(name_prefix=config['name_prefix'], data_frame=df, column_range=crange, con=con)

def _load_masterdata(*, qualified_table_name: str, temp_view_name: str, data_frame: pl.DataFrame, column_range: ColumnRange, con: duckdb.DuckDBPyConnection) -> None: 
    indices = list(range(column_range.range_start, column_range.range_end + 1, 1))
    df1 = data_frame[:, indices]
    df1 = polars_import_utils.normalize_df_column_names(df1)
    con.register(view_name=temp_view_name, python_object=df1)
    sql_stmt = f'CREATE OR REPLACE TABLE {qualified_table_name} AS SELECT * FROM {temp_view_name};'
    con.execute(sql_stmt)
    con.commit()

def _load_values(*, name_prefix: str, data_frame: pl.DataFrame, column_range: ColumnRange, con: duckdb.DuckDBPyConnection) -> None: 
    # use separate tables for equi and floc values
    indices = list(range(column_range.range_start, column_range.range_end + 1, 1))
    table_name = f'{name_prefix}_{column_range.range_name.lower()}'
    indices.insert(0, 1) # add equipment id
    df1 = data_frame[:, indices]
    class_column_name = f'Class {column_range.range_name} is assigned'
    class_column_value = f'{column_range.range_name} is assigned'
    # filter
    df2 = df1.filter(pl.col(class_column_name) == class_column_value)
    # add constant column
    df2 = df2.with_columns(class_name = pl.lit(column_range.range_name))
    df2 = df2.drop([class_column_name])
    if name_prefix == 'valuaequi': 
        df2 = df2.rename({'Equipment': 'entity_id'})
    if name_prefix == 'valuafloc': 
        df2 = df2.rename({'Functional Location': 'entity_id'})
    df2 = polars_import_utils.normalize_df_column_names(df2)
    df2 = polars_import_utils.remove_df_column_name_indices(df2)
    temp_view = f'vw_df_{table_name}'
    con.register(view_name=temp_view, python_object=df2)
    sql_stmt = f'CREATE OR REPLACE TABLE s4_ihx_raw_data.{table_name} AS SELECT * FROM {temp_view};'
    con.execute(sql_stmt)
    con.commit()


