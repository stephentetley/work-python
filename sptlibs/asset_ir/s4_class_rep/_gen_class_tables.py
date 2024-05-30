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
import sptlibs.data_import.import_utils as import_utils

# TODO change to use Jinja

def gen_class_tables(*, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
    for row in _get_equi_classes(con=con).iter_rows(named=True):
        _gen_equiclass_table1(class_name=row['class_name'], con=con)
    for row in _get_floc_classes(con=con).iter_rows(named=True):
        _gen_equiclass_table1(class_name=row['class_name'], con=con)

def _get_equi_classes(*, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
    select_stmt = """
        SELECT DISTINCT ce.class AS class_name 
        FROM s4_fd_raw_data.classequi_classequi1 ce 
        WHERE ce.class NOT IN ('AIB_REFERENCE', 'SOLUTION_ID')
        ORDER BY class_name;
    """
    return con.execute(select_stmt).pl()


def _gen_equiclass_table1(*, class_name: str, con: duckdb.DuckDBPyConnection) -> None:
    get_columns_prepstmt = """
        SELECT 
            ec.char_name AS attr_name,
            CASE 
                WHEN ec.char_type = 'NUM'  THEN IF(ec.char_precision IS NULL, 'INTEGER', format('DECIMAL({}, {})', ec.char_length, ec.char_precision))
                WHEN ec.char_type = 'DATE' THEN 'DATE'
                ELSE 'VARCHAR'
            END AS attr_type, 
        FROM s4_classlists.equi_characteristics ec
        WHERE 
            ec.class_name = ?
    """
    df = con.execute(get_columns_prepstmt, [class_name]).pl()
    table_name = f'equiclass_{class_name.lower()}'   
    ss = [f'CREATE OR REPLACE TABLE s4_class_rep.{table_name} (',
          f'    equipment_id VARCHAR NOT NULL,']
    for row in df.iter_rows(named=True):
        ss.append('    {} {},'.format(import_utils.normalize_name(row['attr_name']), row['attr_type']))
    ss.append(');')
    create_table_stmt = '\n'.join(ss) 
    # print(create_table_stmt)
    con.execute(create_table_stmt)
    con.commit()


def gen_floc_class_tables(*, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
    df = _get_floc_classes(con=con)
    for row in df.iter_rows(named=True):
        _gen_flocclass_table1(class_name=row['class_name'], con=con)


def _get_floc_classes(*, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
    select_stmt = """
        SELECT DISTINCT ce.class AS class_name 
        FROM s4_fd_raw_data.classequi_classequi1 ce 
        WHERE ce.class NOT IN ('AIB_REFERENCE', 'SOLUTION_ID')
        ORDER BY class_name;
    """
    return con.execute(select_stmt).pl()


def _gen_flocclass_table1(*, class_name: str, con: duckdb.DuckDBPyConnection) -> None:
    get_columns_prepstmt = """
        SELECT 
            fc.char_name AS attr_name,
            CASE 
                WHEN fc.char_type = 'NUM'  THEN IF(fc.char_precision IS NULL, 'INTEGER', format('DECIMAL({}, {})', fc.char_length, fc.char_precision))
                WHEN fc.char_type = 'DATE' THEN 'DATE'
                ELSE 'VARCHAR'
            END AS attr_type, 
        FROM s4_classlists.floc_characteristics fc
        WHERE 
            fc.class_name = ?
    """
    df = con.execute(get_columns_prepstmt, [class_name]).pl()
    table_name = f'flocclass_{class_name.lower()}'   
    ss = [f'CREATE OR REPLACE TABLE s4_class_rep.{table_name} (',
          f'    floc_id VARCHAR NOT NULL,']
    for row in df.iter_rows(named=True):
        ss.append('    {} {},'.format(import_utils.normalize_name(row['attr_name']), row['attr_type']))
    ss.append(');')
    create_table_stmt = '\n'.join(ss) 
    # print(create_table_stmt)
    con.execute(create_table_stmt)
    con.commit()

