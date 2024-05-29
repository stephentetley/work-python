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
from jinja2 import Template

def materialize_class_tables(*, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
    for row in _get_equi_classes(con=con).iter_rows(named=True):
        _gen_flocclass_table1(class_name=row['class_name'], con=con)

def _get_equi_classes(*, con: duckdb.DuckDBPyConnection) -> pl.DataFrame: 
    select_stmt = """
        SELECT DISTINCT ce.class AS class_name 
        FROM s4_fd_raw_data.classequi_classequi1 ce 
        WHERE ce.class NOT IN ('AIB_REFERENCE', 'SOLUTION_ID')
        ORDER BY class_name;
    """
    return con.execute(select_stmt).pl()

def _gen_flocclass_table1(*, class_name: str, con: duckdb.DuckDBPyConnection) -> None:
    get_fields_query = Template(_get_fields_template).render(class_name=class_name)
    df = con.execute(get_fields_query).pl()
    chars = [_make_field(char_name=e.get('char_name'), is_num=e.get('is_num')) for e in df.iter_rows(named=True)]
    
    table_name = f'equiclass_{class_name.lower()}'
    insert_into_stmt = Template(_insert_template).render(table_name=table_name, fields=chars, class_name=class_name)
    # print(insert_into_stmt)
    con.execute(insert_into_stmt)
    con.commit()



_get_fields_template = """
    SELECT 
        ch.char_name AS char_name,
        CASE 
            WHEN ch.s4_char_type = 'NUM' THEN true
            ELSE false
        END AS is_num
    FROM s4_classlists.vw_refined_characteristic_defs ch
    WHERE
        class_type = '002'
    AND class_name = '{{class_name}}';
"""

_insert_template = """
    INSERT INTO s4_class_rep.{{table_name}} BY NAME
    SELECT
        ve.equi AS equipment_id, 
        {%- for field in fields %}
        any_value(CASE WHEN ve.charid = '{{field.char_name}}' THEN ve.{{field.value_field}} ELSE NULL END) AS {{field.db_name}},
        {%- endfor %}
    FROM s4_fd_raw_data.valuaequi_valuaequi1 ve
    JOIN s4_fd_raw_data.classequi_classequi1 ce ON ve.equi = ce.equi 
    WHERE ce.class = '{{class_name}}'
    GROUP BY equipment_id;
"""


def _make_field(*, char_name: str, is_num: bool) -> dict:
    value_field = "atflv" if is_num else "atwrt"
    return {'char_name': char_name, 'db_name': char_name.lower(), 'value_field': value_field}


