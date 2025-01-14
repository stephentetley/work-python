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
from sptlibs.utils.sql_script_runner import SqlScriptRunner


def create_duckdb_classlists(*, source_directory: str, con: duckdb.DuckDBPyConnection) -> None:
    if source_directory and os.path.exists(source_directory):
        runner = SqlScriptRunner()
        runner.exec_sql_file(file_rel_path='s4_classlists/s4_classlists_create_tables.sql', con=con)
        sources = _get_classlist_files(source_dir = source_directory)
        for source in sources:
            print(source)
            df = _read_source(source)
            import_utils.duckdb_store_polars_dataframe(df, table_name='s4_classlists.dataframe_temp', con=con)
            runner.exec_sql_file(file_rel_path='s4_classlists/s4_classlists_insert_into.sql', con=con)
                


def _get_classlist_files(*, source_dir: str) -> list[XlsxSource]:
    globlist = glob.glob('*.xlsx', root_dir=source_dir, recursive=False)
    def not_temp(file_name): 
        return not '~$' in file_name
    def expand(file_name): 
        return XlsxSource(os.path.normpath(os.path.join(source_dir, file_name)), 'Sheet1')
    return [expand(e) for e in globlist if not_temp(e)]


def _read_source(src: XlsxSource) -> pl.DataFrame: 
    df = pl.read_excel(source=src.path, 
                       sheet_name=src.sheet, 
                       engine='calamine', 
                       columns=['Class', 'Characteristic', 'Value', 
                                'Char.Value', 'Data Type', 'No. Chars', 
                                'Dec.places'],
                       drop_empty_rows=True)
    df = import_utils.normalize_df_column_names(df) 
    df = df.with_columns(pl.col(pl.String).replace("", None))
    df = df.filter(pl.any_horizontal(pl.col("*").is_not_null())).with_row_index(name="row_idx")
    return df


def copy_classlists_tables(*, classlists_source_db_path: str, setup_tables: bool, dest_con: duckdb.DuckDBPyConnection) -> None:
    """`dest_con` is the desination database."""
    # Setup tables
    if setup_tables: 
        runner = SqlScriptRunner()
        runner.exec_sql_file(file_rel_path='s4_classlists/s4_classlists_create_tables.sql', con=dest_con)
    # copy tables using duckdb builtins
    import_utils.duckdb_import_tables_from_duckdb(
        source_db_path=classlists_source_db_path, 
        dest_con=dest_con,
        source_and_dest_tables=_copy_tables_srcs_dests)

_copy_tables_srcs_dests = {
    's4_classlists.floc_characteristics': 's4_classlists.floc_characteristics',
    's4_classlists.floc_enums': 's4_classlists.floc_enums', 
    's4_classlists.equi_characteristics': 's4_classlists.equi_characteristics',
    's4_classlists.equi_enums': 's4_classlists.equi_enums'
}
