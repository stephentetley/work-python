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
import sptlibs.data_import.classlists.classlist_parser as classlist_parser
import sptlibs.data_import.classlists.duckdb_setup as duckdb_setup

def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    duckdb_setup.setup_tables(con=con)


def import_floc_classes(textfile_path: str, *, con: duckdb.DuckDBPyConnection) -> None:
    (df_chars, df_enums) = classlist_parser.parse_floc_classfile(textfile_path)
    con.register(view_name='vw_df_chars', python_object=df_chars)
    insert_chars_stmt = """
        INSERT INTO s4_classlists.floc_characteristics BY NAME
        SELECT 
            df.class_name AS class_name,
            df.char_name AS char_name,
            df.class_description AS class_description,
            df.char_description AS char_description,
            df.char_type AS char_type,
            df.char_length AS char_length,
            df.char_precision AS char_precision
        FROM vw_df_chars df;
        """
    con.execute(insert_chars_stmt)
    con.commit()
    con.register(view_name='vw_df_enums', python_object=df_enums)
    insert_enums_stmt = """
        INSERT INTO s4_classlists.floc_enums BY NAME
        SELECT 
            df.class_name AS class_name,
            df.char_name AS char_name,
            df.enum_value AS enum_value,
            df.enum_description AS enum_description
        FROM vw_df_enums df;
        """
    con.execute(insert_enums_stmt)
    con.commit()

def import_equi_classes(textfile_path: str, *, con: duckdb.DuckDBPyConnection) -> None:
    (df_chars, df_enums) = classlist_parser.parse_equi_classfile(textfile_path)
    con.register(view_name='vw_df_chars', python_object=df_chars)
    insert_chars_stmt = """
        INSERT INTO s4_classlists.equi_characteristics BY NAME
        SELECT 
            df.class_name AS class_name,
            df.char_name AS char_name,
            df.class_description AS class_description,
            df.char_description AS char_description,
            df.char_type AS char_type,
            df.char_length AS char_length,
            df.char_precision AS char_precision
        FROM vw_df_chars df;
        """
    con.execute(insert_chars_stmt)
    con.commit()
    con.register(view_name='vw_df_enums', python_object=df_enums)
    insert_enums_stmt = """
        INSERT INTO s4_classlists.equi_enums BY NAME
        SELECT 
            df.class_name AS class_name,
            df.char_name AS char_name,
            df.enum_value AS enum_value,
            df.enum_description AS enum_description
        FROM vw_df_enums df;
        """
    con.execute(insert_enums_stmt)
    con.commit()