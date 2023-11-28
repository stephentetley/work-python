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
import pandas as pd
import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.import_utils as import_utils
from sptlibs.ih06_ih08.column_range import ColumnRange


def load_ih08(*, xlsx_src: XlsxSource, con: duckdb.DuckDBPyConnection) -> None:
    df = pd.read_excel(xlsx_src.path, xlsx_src.sheet)
    re_class_start = re.compile(r"Class (?P<class_name>[\w_]+) is assigned")
    con.execute('CREATE SCHEMA IF NOT EXISTS s4_raw_data;'),
    ranges = []
    # start at column 1, dropping column 0 `selected line`
    range1 = ColumnRange(range_name='equi_masterdata', start=1)
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
    _load_equi_masterdata(df, ranges[0], con)
    for crange in ranges[1:]:
        _load_equi_values(df, crange, con)

def _load_equi_masterdata(df: pd.DataFrame, cr: ColumnRange, con: duckdb.DuckDBPyConnection) -> None: 
    indices = list(range(cr.range_start, cr.range_end + 1, 1))
    df1 = df.iloc[:, indices]
    df1 = import_utils.normalize_df_column_names(df1)
    con.register(view_name='vw_df_equi', python_object=df1)
    sql_stmt = f'CREATE TABLE s4_raw_data.equi_masterdata AS SELECT * FROM vw_df_equi;'
    con.execute(sql_stmt)
    con.commit()

def _load_equi_values(df: pd.DataFrame, cr: ColumnRange, con: duckdb.DuckDBPyConnection) -> None: 
    # use separate tables for equi and floc values
    indices = list(range(cr.range_start, cr.range_end + 1, 1))
    table_name = 'valuaequi_%s' % cr.range_name.lower()
    indices.insert(0, 1) # add equipment id
    df1 = df.iloc[:, indices]
    class_column_name = 'Class %s is assigned' % cr.range_name
    class_column_value = '%s is assigned' % cr.range_name
    # filter
    df2 = df1[df1[class_column_name] == class_column_value].copy(deep=True)
    # add constant column
    df2['class_name'] = cr.range_name
    df2 = df2.drop([class_column_name], axis=1)
    df2.rename(columns={'Equipment': 'entity_id'}, inplace=True)
    df2 = import_utils.normalize_df_column_names(df2)
    df2 = import_utils.remove_df_column_name_indices(df2)
    temp_view = f'vw_df_{table_name}'
    con.register(view_name=temp_view, python_object=df2)
    sql_stmt = f'CREATE TABLE s4_raw_data.{table_name} AS SELECT * FROM {temp_view};'
    con.execute(sql_stmt)
    con.commit()


