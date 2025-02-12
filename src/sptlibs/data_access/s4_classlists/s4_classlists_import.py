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

import os
import glob
import duckdb
import polars as pl
import sptlibs.data_access.import_utils as import_utils
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.utils.sql_script_runner2 import SqlScriptRunner2


# TODO - change source to list[str]
def duckdb_import(*, sources: list[XlsxSource], con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner2(__file__, con=con)
    runner.exec_sql_file(rel_file_path='s4_classlists_create_tables.sql', con=con)
    for source in sources:
        print(source.path)
        df = _read_source(source)
        import_utils.duckdb_store_polars_dataframe(df, table_name='s4_classlists.dataframe_temp', con=con)
        runner.exec_sql_file(rel_file_path='s4_classlists_insert_into.sql', con=con)
                

# use `schema_overrides` because input source iss too sparse with long 
# blank prefixes to columns
def _read_source(src: XlsxSource) -> pl.DataFrame: 
    df = pl.read_excel(source=src.path, 
                       sheet_name=src.sheet, 
                       engine='calamine', 
                       columns=['Class', 'Characteristic', 'Value', 
                                'Char.Value', 'Data Type', 'No. Chars', 
                                'Dec.places'],
                       schema_overrides={"Class": pl.String,
                                         'Characteristic': pl.String,
                                         'Value': pl.String,
                                         'Char.Value': pl.String,
                                         'Data Type': pl.String,
                                         'No. Chars': pl.Int32, 
                                         'Dec.places': pl.Int32},
                       drop_empty_rows=True)
    df = import_utils.normalize_df_column_names(df) 
    df = df.with_columns(pl.col(pl.String).replace("", None))
    df = df.filter(pl.any_horizontal(pl.col("*").is_not_null())).with_row_index(name="row_idx")
    return df


def copy_classlists_tables(*, classlists_source_db_path: str, setup_tables: bool, dest_con: duckdb.DuckDBPyConnection) -> None:
    """`dest_con` is the desination database."""
    # classlists includes views that we need to create
    runner = SqlScriptRunner2(__file__, con=dest_con)
    runner.exec_sql_file(rel_file_path='s4_classlists_create_tables.sql')
    import_utils.duckdb_import_tables_from_duckdb(
        source_db_path=classlists_source_db_path, 
        con=dest_con,
        schema_name='s4_classlists',
        create_schema=True,
        source_tables= ['s4_classlists.floc_characteristics',
                        's4_classlists.floc_enums', 
                        's4_classlists.equi_characteristics',
                        's4_classlists.equi_enums'])

