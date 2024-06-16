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

import duckdb
from jinja2 import Template
import sptlibs.data_import.s4_classlists._parser as _parser
import sptlibs.data_import.import_utils as import_utils
from sptlibs.utils.sql_script_runner import SqlScriptRunner
from sptlibs.utils.asset_data_config import AssetDataConfig


def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='s4_classlists/s4_classlists_create_tables.sql', con=con)


def import_floc_classes(textfile_path: str, *, con: duckdb.DuckDBPyConnection) -> None:
    (df_chars, df_enums) = _parser.parse_floc_classfile(textfile_path)
    import_utils.duckdb_write_dataframe_to_table(
             df_chars, con=con, 
             qualified_table_name='s4_classlists.floc_characteristics', 
             columns_and_aliases=_characteristic_table_columns)
    import_utils.duckdb_write_dataframe_to_table(
             df_enums, con=con, 
             qualified_table_name='s4_classlists.floc_enums', 
             columns_and_aliases=_enum_table_columns)
    
def import_equi_classes(textfile_path: str, *, con: duckdb.DuckDBPyConnection) -> None:
    (df_chars, df_enums) = _parser.parse_equi_classfile(textfile_path)
    import_utils.duckdb_write_dataframe_to_table(
             df_chars, con=con, 
             qualified_table_name='s4_classlists.equi_characteristics', 
             columns_and_aliases=_characteristic_table_columns)
    import_utils.duckdb_write_dataframe_to_table(
             df_enums, con=con, 
             qualified_table_name='s4_classlists.equi_enums', 
             columns_and_aliases=_enum_table_columns)
    
_characteristic_table_columns = {
    'class_name': 'class_name',
    'char_name': 'char_name',
    'class_description': 'class_description',
    'char_description': 'char_description',
    'char_type': 'char_type',
    'char_length': 'char_length',
    'char_precision': 'char_precision'
}


_enum_table_columns = {
    'class_name': 'class_name',
    'char_name': 'char_name',
    'enum_value': 'enum_value',
    'enum_description': 'enum_description'
}

def copy_standard_classlists_tables(*, con: duckdb.DuckDBPyConnection) -> None:
    config = AssetDataConfig()
    config.set_focus('file_download_summary')
    classlists_db = config.get_expanded_path('classlists_db_src')
    copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=con)


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