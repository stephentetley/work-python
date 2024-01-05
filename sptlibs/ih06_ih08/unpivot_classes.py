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


def unpivot_classes(*, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(_create_normalize_column_name_macro)
    _make_ih_char_and_classes1(table_prefix='valuaequi', class_type='002', con=con)
    _make_ih_char_and_classes1(table_prefix='valuafloc', class_type='003', con=con)

def _make_ih_char_and_classes1(*, table_prefix: str, class_type: str, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(make_valua_tables_query(table_prefix=table_prefix))
    for row in con.fetchall():
         qualified_name = f'{row[0]}.{row[1]}'
         ins1 = make_s4_classes_insert(qualified_table_name=qualified_name, class_type=class_type)
         con.execute(ins1)
         ins2 = make_s4_char_values_insert(qualified_table_name=qualified_name, class_type=class_type)
         con.execute(ins2)
    con.commit()


_create_normalize_column_name_macro = """
    CREATE OR REPLACE MACRO normalize_column_name(name) AS regexp_replace(trim(regexp_replace(lower(name), '[\W+]', ' ', 'g')), '[\W]+', '_', 'g');
    """

def make_valua_tables_query(*, table_prefix: str) -> str: 
    return f"""
    SELECT DISTINCT
        dc.schema_name,
        dc.table_name AS table_name,
    FROM duckdb_columns() dc
    WHERE dc.schema_name = 's4_ihx_raw_data'
    AND dc.table_name LIKE '{table_prefix}_%';
    """

def make_s4_classes_insert(*, qualified_table_name: str, class_type: str) -> str:
     return f"""
    INSERT INTO s4_summary.class_values BY NAME
    SELECT DISTINCT
        vals.entity_id AS entity_id,
        vals.class_name AS class_name,
        '{class_type}' AS class_type,
    FROM (UNPIVOT {qualified_table_name}
    ON COLUMNS(* EXCLUDE (entity_id, class_name))
    INTO 
        NAME attribute_name
        VALUE attribute_value) vals;
    """

def make_s4_char_values_insert(*, qualified_table_name: str, class_type: str) -> str:
     return f"""
    INSERT INTO s4_summary.char_values BY NAME
    SELECT 
        vals.entity_id AS entity_id,
        vals.class_name AS class_name,
        '{class_type}' AS class_type,
        cd.char_name AS char_name,
        CASE 
            WHEN cd.refined_char_type = 'TEXT' THEN vals.attribute_value
            WHEN cd.refined_char_type = 'DATE' THEN vals.attribute_value
            ELSE NULL
        END AS char_text_value,
        CASE 
            WHEN cd.refined_char_type = 'INTEGER' THEN TRY_CAST(vals.attribute_value AS INTEGER)
            ELSE NULL
        END AS char_integer_value,
        CASE 
            WHEN cd.refined_char_type = 'DECIMAL' THEN TRY_CAST(vals.attribute_value AS DECIMAL)
            ELSE NULL
        END AS char_decimal_value,
    FROM (UNPIVOT {qualified_table_name}
    ON COLUMNS(* EXCLUDE (entity_id, class_name))
    INTO 
        NAME attribute_name
        VALUE attribute_value) vals
    LEFT OUTER JOIN s4_classlists.vw_refined_characteristic_defs cd ON normalize_column_name(cd.char_description) = vals.attribute_name AND cd.class_name = vals.class_name
    WHERE cd.class_type = '{class_type}'
    ORDER BY entity_id;
    """
