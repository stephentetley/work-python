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


def _trafo_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([ 
        (pl.col("os_name").str.strip_chars()),
        (pl.col("od_name").str.strip_chars()),
        (pl.col("os_addr").str.strip_chars().str.replace(',\s*', '_')),
        (pl.col("os_type").str.strip_chars()),
        (pl.col("os_comment").str.strip_chars()),
        (pl.col("od_comment").str.strip_chars()),
        (pl.col("media").str.strip_chars()),
        (pl.col("scan_sched").str.strip_chars()),
        (pl.col("set_name").str.strip_chars()),
        (pl.col("parent_ou").str.strip_chars()),
        (pl.col("parent_ou_comment").str.strip_chars()),
        (pl.col("last_polled").str.strip_chars().str.to_datetime("%H:%M %d-%b-%y", strict=False)),
        (pl.col("last_power_up").str.strip_chars().str.to_datetime("%H:%M %d-%b-%y", strict=False)),
    ])

def duckdb_import(rts_report_csv: str, *, con: duckdb.DuckDBPyConnection) -> None:
    con.execute('CREATE SCHEMA IF NOT EXISTS rts_raw_data;')
    import_utils.duckdb_import_csv(
        rts_report_csv, 
        separator='\t',
        table_name='rts_raw_data.outstations_report', 
        rename_before_trafo=True, 
        df_trafo=_trafo_dataframe, 
        con=con)

