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
import pandas as pd
import sptlibs.export_utils as export_utils
from sptlibs.data_frame_xlsx_table import DataFrameXlsxTable


def make_summary_report(*, output_xls: str, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(_tabname_macro)
    tabs = []
    # valuafloc tabs
    con.execute(_make_valua_data_query(class_type='003'))
    for row in con.fetchall():
        tab_name = row[2]
        tab_query = _make_valuafloc_pivot_query(class_name=row[1], columns=row[3])
        tabs.append((tab_name, tab_query))
    # valuaequi tabs
    con.execute(_make_valua_data_query(class_type='002'))
    for row in con.fetchall():
        tab_name = row[2]
        tab_query = _make_valuaequi_pivot_query(class_name=row[1], columns=row[3])
        tabs.append((tab_name, tab_query))
    with pd.ExcelWriter(output_xls) as xlwriter: 
        # funcloc masterdata
        con.execute('SELECT md.* FROM s4_ih_funcloc_masterdata md ORDER BY md.functional_location;')
        df_floc = con.df()
        table_writer = DataFrameXlsxTable(df=df_floc)
        table_writer.to_excel(writer=xlwriter, sheet_name='functional_location')
        # equipment masterdata
        con.execute('SELECT md.* FROM s4_ih_equipment_masterdata md ORDER BY md.functional_location;')
        df_equi = con.df()
        table_writer = DataFrameXlsxTable(df=df_equi)
        table_writer.to_excel(writer=xlwriter, sheet_name='equipment')
        for (name, query) in tabs:
            print(name)
            con.execute(query)
            df = con.df()
            # TODO need to simplify list output for characteristics columns
            table_writer = DataFrameXlsxTable(df=df)
            table_writer.to_excel(writer=xlwriter, sheet_name=name)
        

_tabname_macro = """
    CREATE OR REPLACE MACRO tabname(clstype, clsname) AS CASE
        WHEN clstype = '002' THEN 'e.' || lower(clsname)
        WHEN clstype = '003' THEN 'f.' || lower(clsname)
        ELSE clstype || lower(clsname)
    END;
    """

def _make_valua_data_query(*, class_type: str) -> str:
    return f"""
    SELECT 
        defs.class_type AS class_type,
        defs.class_name AS class_name,
        tabname(defs.class_type, defs.class_name) AS tab_name,
        defs.columns AS columns,
    FROM 
        (SELECT
            cd.class_type AS class_type,
            cd.class_name AS class_name,
            list(cd.char_name) AS columns,
        FROM s4_classlists.characteristic_defs cd
        WHERE class_type = '{class_type}'
        GROUP BY ALL) defs  
    JOIN (SELECT DISTINCT class_name FROM main.s4_ih_classes) cls_used ON cls_used.class_name = defs.class_name
    ORDER BY defs.class_type, defs.class_name;
    """

def _make_valuafloc_pivot_query(*, class_name: str, columns: list) -> str:
    pv_selectors = _make_select_lines(columns)
    quoted_char_names = _make_quoted_name_list(columns)
    return f"""
        SELECT 
            fm.functional_location AS functional_location,
            fm.description AS description,
            strftime(fm.startup_date, '%Y.%m.%d') AS startup_date,
            fm.structure_indicator AS structure_indicator,
            fm.object_type AS object_type,
            fm.user_status AS user_status,
            {pv_selectors}
        FROM (PIVOT (
            SELECT vals.entity_id, vals.class_name, vals.char_name, vals.text_value AS attr_value FROM s4_ih_char_values vals WHERE vals.class_name = '{class_name}' AND vals.text_value IS NOT NULL 
            UNION
            SELECT vals.entity_id, vals.class_name, vals.char_name, vals.numeric_value::TEXT AS attr_value FROM s4_ih_char_values vals WHERE vals.class_name = '{class_name}' AND vals.numeric_value IS NOT NULL
            ) 
        ON char_name IN ({quoted_char_names}) USING list(attr_value)) pv
        JOIN main.s4_ih_funcloc_masterdata fm ON fm.functional_location = pv.entity_id
        ORDER BY fm.functional_location; 
        """

def _make_valuaequi_pivot_query(*, class_name: str, columns: list) -> str:
    pv_selectors = _make_select_lines(columns)
    quoted_char_names = _make_quoted_name_list(columns)
    return f"""
        SELECT 
            em.equi_id AS equipment_id,
            em.description AS equipment_name,
            em.functional_location AS functional_location,
            em.manufacturer AS manufacturer,
            em.model_number AS model_number,
            strftime(em.startup_date, '%Y.%m.%d') AS startup_date,
            em.user_status AS user_status,
            {pv_selectors}
        FROM (PIVOT (
            SELECT vals.entity_id, vals.class_name, vals.char_name, vals.text_value AS attr_value FROM s4_ih_char_values vals WHERE vals.class_name = '{class_name}' AND vals.text_value IS NOT NULL 
            UNION
            SELECT vals.entity_id, vals.class_name, vals.char_name, vals.numeric_value::TEXT AS attr_value FROM s4_ih_char_values vals WHERE vals.class_name = '{class_name}' AND vals.numeric_value IS NOT NULL
            ) 
        ON char_name IN ({quoted_char_names}) USING list(attr_value)) pv
        JOIN main.s4_ih_equipment_masterdata em ON em.equi_id = pv.entity_id
        ORDER BY em.functional_location; 
        """

def _make_select_lines(columns: list) -> str:
    return '\n'.join(map(lambda name: f'    pv.{name},', columns))

def _make_quoted_name_list(columns: list) -> str:
    return ', '.join(map(lambda name: f'\'{name}\'', columns))
