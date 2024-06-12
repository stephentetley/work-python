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
from sptlibs.utils.sql_script_runner import SqlScriptRunner
from sptlibs.utils.asset_data_config import AssetDataConfig


def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='s4_classlists/s4_classlists_create_tables.sql', con=con)


def import_floc_classes(textfile_path: str, *, con: duckdb.DuckDBPyConnection) -> None:
    (df_chars, df_enums) = _parser.parse_floc_classfile(textfile_path)
    # chars
    con.register(view_name='vw_df_chars', python_object=df_chars)
    con.execute(Template(_insert_chars_template).render(table_name='floc_characteristics', dataframe_name='vw_df_chars'))
    con.commit()
    # enums
    con.register(view_name='vw_df_floc_enums', python_object=df_enums)
    con.execute(Template(_insert_enums_template).render(table_name='floc_enums', dataframe_name='vw_df_floc_enums'))
    con.commit()

def import_equi_classes(textfile_path: str, *, con: duckdb.DuckDBPyConnection) -> None:
    (df_chars, df_enums) = _parser.parse_equi_classfile(textfile_path)
    # chars
    con.register(view_name='vw_df_chars', python_object=df_chars)
    con.execute(Template(_insert_chars_template).render(table_name='equi_characteristics', dataframe_name='vw_df_chars'))
    con.commit()
    # enums
    con.register(view_name='vw_df_enums', python_object=df_enums)
    con.execute(Template(_insert_enums_template).render(table_name='equi_enums', dataframe_name='vw_df_enums'))
    con.commit()

_insert_chars_template = """
    INSERT INTO s4_classlists.{{table_name}} BY NAME
    SELECT 
        df.class_name AS class_name,
        df.char_name AS char_name,
        df.class_description AS class_description,
        df.char_description AS char_description,
        df.char_type AS char_type,
        df.char_length AS char_length,
        df.char_precision AS char_precision
    FROM {{dataframe_name}} df;
"""


_insert_enums_template = """
    INSERT INTO s4_classlists.{{table_name}} BY NAME
    SELECT 
        df.class_name AS class_name,
        df.char_name AS char_name,
        df.enum_value AS enum_value,
        df.enum_description AS enum_description
    FROM {{dataframe_name}} df;
"""

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
    dest_con.execute(Template(_copy_tables_template).render(db_source_path=classlists_source_db_path))
    dest_con.commit()

_copy_tables_template = """
    ATTACH '{{db_source_path}}' AS classlists_source;
    INSERT INTO s4_classlists.floc_characteristics SELECT * FROM classlists_source.s4_classlists.floc_characteristics;
    INSERT INTO s4_classlists.floc_enums SELECT * FROM classlists_source.s4_classlists.floc_enums;
    INSERT INTO s4_classlists.equi_characteristics SELECT * FROM classlists_source.s4_classlists.equi_characteristics;
    INSERT INTO s4_classlists.equi_enums SELECT * FROM classlists_source.s4_classlists.equi_enums;
    DETACH classlists_source;
"""