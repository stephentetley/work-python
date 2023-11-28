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
import duckdb


def make_vw_s4_valuaequi_eav(*, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(_equiclasses_used_query)
    rows = con.fetchall()
    ls = list(map(lambda row: row[0], rows))
    make_vw_ddl = vw_s4_valuaequi_eav_ddl(valua_table_names=ls)
    con.execute(make_vw_ddl)

_equiclasses_used_query = """
    SELECT DISTINCT
        scu.table_name AS table_name
    FROM 
        vw_s4_classes_used scu
    WHERE 
        scu.table_name LIKE 'valuaequi_%';
    """ 

def vw_s4_valuaequi_eav_ddl(*, valua_table_names: list) -> str:
    line1 = 'CREATE OR REPLACE VIEW vw_s4_valuaequi_eav AS'
    sep = os.linesep + 'UNION' + os.linesep
    stmts = list(map(lambda name: _unpivot_valua1(table_name=name), valua_table_names))
    sql_all = line1 + sep.join(stmts) + ";"
    return sql_all

def _unpivot_valua1(*, table_name: str) -> str: 
    return f"""
    UNPIVOT s4_raw_data.{table_name}
    ON COLUMNS(* EXCLUDE (entity_id, class_name))
    INTO 
        NAME attribute_name
        VALUE attribute_value
    """


def make_select_numeric_characteristic(*, class_name: str, class_type: str, char_name: str, simple_data_type: str, ddl_data_type: str, column_name: str) -> str:
    match simple_data_type:
        case 'TEXT':
            return _make_select_text_characteristic(class_name=class_name, class_type=class_type, char_name=char_name, column_name=column_name)
        case 'DATE':
            return _make_select_date_characteristic(class_name=class_name, class_type=class_type, char_name=char_name, column_name=column_name)
        case 'NUMERIC':
            return _make_select_text_characteristic(class_name=class_name, class_type=class_type, char_name=char_name, number_type=ddl_data_type, column_name=column_name)
        case _:
            return None


def _make_select_text_characteristic(*, class_name: str, class_type: str, char_name: str, column_name: str) -> str:
    table_prefix = 'valuaequi' if class_type == '002' else 'valuafloc'
    table_name = f'{table_prefix}_{class_name.lower()}'
    return f"""
    SELECT 
        t.entity_id AS entity_id,
        '{class_type}' AS class_type,
        '{class_name}' AS class_name,
        '{char_name}' AS char_name,
        t.{column_name} AS text_value,
        NULL AS numeric_value,
    FROM s4_raw_data.{table_name} t
    WHERE t.{column_name} NOT NULL
    """

def _make_select_date_characteristic(*, class_name: str, class_type: str, char_name: str, column_name: str) -> str:
    table_prefix = 'valuaequi' if class_type == '002' else 'valuafloc'
    table_name = f'{table_prefix}_{class_name.lower()}'
    return f"""
    SELECT 
        t.entity_id AS entity_id,
        '{class_type}' AS class_type,
        '{class_name}' AS class_name,
        '{char_name}' AS char_name,
        t.{column_name} AS text_value,
        NULL AS numeric_value,
    FROM s4_raw_data.{table_name} t
    WHERE t.{column_name} NOT NULL
    """

def _make_select_numeric_characteristic(*, class_name: str, class_type: str, char_name: str, ddl_data_type: str, column_name: str) -> str:
    table_prefix = 'valuaequi' if class_type == '002' else 'valuafloc'
    table_name = f'{table_prefix}_{class_name.lower()}'
    return f"""
    SELECT 
        t.entity_id AS entity_id,
        '{class_type}' AS class_type,
        '{class_name}' AS class_name,
        '{char_name}' AS char_name,
        NULL AS text_value,
        CAST(t.{column_name} AS {ddl_data_type}) AS numeric_value,
    FROM s4_raw_data.{table_name} t
    WHERE t.{column_name} NOT NULL
    """