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


from dataclasses import dataclass
import duckdb
import polars as pl
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_access.import_utils as import_utils
from sptlibs.utils.sql_script_runner2 import SqlScriptRunner2


# TODO rename colums at some point...
def duckdb_import(*, equipment_attributes_source: XlsxSource, con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner2(__file__, con=con)
    runner.exec_sql_file(rel_file_path='setup_ai2_metadata.sql')
    df = import_utils.read_xlsx_source(equipment_attributes_source, normalize_column_names=True)
    aliases = {'assettypecode': 'assettypecode', 
               'assettypedescription': 'assettypedescription',
               'attributename': 'attributename',
               'attributedescription': 'attributedescription',
               'datecreated_attribute': 'datecreated_attribute',
               'category': 'category'}
    import_utils.duckdb_write_dataframe_to_table(df,
                                                 qualified_table_name='ai2_metadata.equipment_attributes',
                                                 con=con, 
                                                 columns_and_aliases=aliases)




def copy_ai2_metadata_tables(*, source_db_path: str, dest_con: duckdb.DuckDBPyConnection) -> None:
    """`dest_con` is the desination database."""
    # copy tables using duckdb builtins
    import_utils.duckdb_import_tables_from_duckdb(source_db_path=source_db_path, 
                                                  con=dest_con,
                                                  schema_name='ai2_metadata',
                                                  source_tables=['ai2_metadata.equipment_attributes'])
