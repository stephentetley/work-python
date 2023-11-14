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
from sptlibs.xlsx_source import XlsxSource
import sptlibs.import_utils as import_utils
from sptlibs.ih06_ih08.column_range import ColumnRange

def _make_equi_tables(df: pd.DataFrame, cr: ColumnRange) -> dict: 
    indices = list(range(cr.range_start, cr.range_end + 1, 1))
    if cr.range_name == 'equi_masterdata':
        df1 = df.iloc[:, indices]
        df1 = import_utils.normalize_df_column_names(df1)
        return {'table_name': 'equi_masterdata', 'data_frame': df1}
    else:
        # use separate tables for equi and floc values
        table_name = 'equichars_%s' % cr.range_name.lower()
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
        return {'table_name': table_name, 'data_frame': df2}
    
def parse_ih08(*, xlsx_src: XlsxSource) -> list:
    df = pd.read_excel(xlsx_src.path, xlsx_src.sheet)
    re_class_start = re.compile(r"Class (?P<class_name>[\w_]+) is assigned")

    ranges = []
    # start at column 1 to drop column 0 `selected line`
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

    tables = map(lambda range1: _make_equi_tables(df, range1), ranges)
    return tables

