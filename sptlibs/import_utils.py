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
import sqlite3
from typing import Callable
from sptlibs.xlsx_source import XlsxSource


def import_sheet(source: XlsxSource, *, table_name: str, con: sqlite3.Connection, if_exists: str, df_trafo: Callable[[pd.DataFrame], pd.DataFrame]) -> None:
    '''Note drops the table `table_name` before filling it'''
    xlsx = pd.ExcelFile(source.path)
    df_raw = pd.read_excel(xlsx, source.sheet)
    if df_trafo is not None:
        df_clean = df_trafo(df_raw)
    else:
        df_clean = df_raw
    df_renamed = normalize_df_column_names(df_clean)
    df_renamed.to_sql(name=table_name, if_exists=if_exists, con=con)
    con.commit()

def normalize_df_column_names(df):
    return df.rename(columns = lambda s: normalize_name(s))


def normalize_name(s):
    ls = s.lower()
    remove_suffix = re.sub(r'[\W]+$' , '', ls)
    remove_bad = re.sub(r'[\W]+' , '_', remove_suffix)
    return remove_bad

