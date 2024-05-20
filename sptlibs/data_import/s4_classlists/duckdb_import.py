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
import sptlibs.data_import.s4_classlists._parser as _parser
import sptlibs.data_import.s4_classlists._dbsetup as _dbsetup

def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    _dbsetup.setup_tables(con=con)

def import_floc_classes(textfile_path: str, *, con: duckdb.DuckDBPyConnection) -> None:
    (df_chars, df_enums) = _parser.parse_floc_classfile(textfile_path)
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
    (df_chars, df_enums) = _parser.parse_equi_classfile(textfile_path)
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

def copy_classlists_tables(*, classlists_source_db_path: str, setup_tables: bool, dest_con: duckdb.DuckDBPyConnection) -> None:
    """`dest_con` is the desination database."""
    # Setup tables
    if setup_tables: 
        _dbsetup.setup_tables(con=dest_con)

    # copy tables using duckdb builtins
    copy_tables_sql = f"""
        ATTACH '{classlists_source_db_path}' AS classlists_source;
        INSERT INTO s4_classlists.floc_characteristics SELECT * FROM classlists_source.s4_classlists.floc_characteristics;
        INSERT INTO s4_classlists.floc_enums SELECT * FROM classlists_source.s4_classlists.floc_enums;
        INSERT INTO s4_classlists.equi_characteristics SELECT * FROM classlists_source.s4_classlists.equi_characteristics;
        INSERT INTO s4_classlists.equi_enums SELECT * FROM classlists_source.s4_classlists.equi_enums;
        DETACH classlists_source;
    """
    dest_con.execute(copy_tables_sql)
    dest_con.commit()
