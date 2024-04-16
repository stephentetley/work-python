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




def gen_cr_table(*, pk_name: str, schema_name: str, class_name: str, con: duckdb.DuckDBPyConnection) -> None:
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
    # TODO equi|floc
    table_name = f'equi_{class_name.lower()}'   
    ss = [f'CREATE OR REPLACE TABLE {schema_name}.{table_name} (',
          f'    {pk_name} VARCHAR NOT NULL,']
    for row in df.iter_rows(named=True):
        ss.append('    {} {},'.format(import_utils.normalize_name(row['attr_name']), row['attr_type']))
    ss.append(f'    PRIMARY KEY({pk_name})')
    ss.append(');')
    create_table_stmt = '\n'.join(ss) 
    # print(create_table_stmt)
    con.execute(create_table_stmt)
    con.commit()

