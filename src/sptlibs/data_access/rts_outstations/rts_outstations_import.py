"""
Copyright 2024 Stephen Tetley

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


import duckdb
import polars as pl
import sptlibs.data_access.import_utils as import_utils
import polars.selectors as cs

def _trafo_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    df = df.select(pl.all().str.strip_chars()) 
    df = df.with_columns(
        cs.by_name(['last_polled', 'last_power_up'], require_all=False).str.to_datetime("%H:%M %d-%b-%y", strict=False)
    )
    df = df.select(
        cs.by_name(['os_name', 'od_name', 'od_comment', 'os_comment', 
                    'scan_status', 'last_polled', 'last_power_up', 'set_name',
                    'media_type', 'ip_address', 'os_address', 'os_type'], require_all=False)
    )
    return df

# TODO import_utils is a mess
def duckdb_import(rts_report_csv: str, *, con: duckdb.DuckDBPyConnection) -> None:
    con.execute('CREATE SCHEMA IF NOT EXISTS rts_raw_data;')
    import_utils.duckdb_import_csv(
        rts_report_csv, 
        separator='\t',
        table_name='rts_raw_data.outstations_report', 
        rename_before_trafo=True, 
        df_trafo=_trafo_dataframe, 
        con=con)

