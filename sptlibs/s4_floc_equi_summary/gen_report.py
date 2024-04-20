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
import pandas as pd
from sptlibs.data_frame_xlsx_table import DataFrameXlsxTable
import sptlibs.unjson as unjson


def gen_report(*, xls_output_path: str, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(_create_tabname_macro)
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
    with pd.ExcelWriter(xls_output_path) as xlwriter: 
        # funcloc masterdata
        con.execute(_funcloc_masterdata_query)
        df_floc = con.df()
        table_writer = DataFrameXlsxTable(df=df_floc)
        table_writer.to_excel(writer=xlwriter, sheet_name='functional_location')
        # equipment masterdata
        con.execute(_equipment_masterdata_query)
        df_equi = con.df()
        table_writer = DataFrameXlsxTable(df=df_equi)
        table_writer.to_excel(writer=xlwriter, sheet_name='equipment')
        for (name, query) in tabs:
            print(name)
            con.execute(query)
            df = con.df()
            df1 = unjson.pp_json_columns(df)
            # TODO need to simplify list output for characteristics columns
            table_writer = DataFrameXlsxTable(df=df1)
            table_writer.to_excel(writer=xlwriter, sheet_name=name)
        

_create_tabname_macro = """
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
            '002' AS class_type,
            cd.class_name AS class_name,
            list(cd.char_name) AS columns,
        FROM s4_classlists.equi_characteristics cd
        WHERE class_type = '{class_type}'
        GROUP BY ALL) defs  
    JOIN (SELECT DISTINCT class_name FROM s4_summary.class_values) cls_used ON cls_used.class_name = defs.class_name
    ORDER BY defs.class_type, defs.class_name;
    """

def _make_valuafloc_pivot_query(*, class_name: str, columns: list) -> str:
    pv_selectors = _make_select_lines(columns)
    quoted_char_names = _make_quoted_name_list(columns)
    return f"""
        SELECT 
            fm.floc_id AS floc_id,
            fm.functional_location AS functional_location,
            fm.description AS description,
            strftime(fm.startup_date, '%Y.%m.%d') AS startup_date,
            fm.structure_indicator AS structure_indicator,
            fm.object_type AS object_type,
            fm.user_status AS user_status,
            {pv_selectors}
        FROM (PIVOT (
            SELECT vals.entity_id, vals.class_name, vals.char_name, vals.char_text_value AS attr_value FROM s4_summary.char_values vals WHERE vals.class_name = '{class_name}' AND vals.char_text_value IS NOT NULL 
            UNION
            SELECT vals.entity_id, vals.class_name, vals.char_name, vals.char_integer_value::TEXT AS attr_value FROM s4_summary.char_values vals WHERE vals.class_name = '{class_name}' AND vals.char_integer_value IS NOT NULL
            UNION
            SELECT vals.entity_id, vals.class_name, vals.char_name, vals.char_decimal_value::TEXT AS attr_value FROM s4_summary.char_values vals WHERE vals.class_name = '{class_name}' AND vals.char_decimal_value IS NOT NULL
            ) 
        ON char_name IN ({quoted_char_names}) USING list(attr_value)) pv
        JOIN s4_summary.funcloc_masterdata fm ON fm.floc_id = pv.entity_id
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
            SELECT vals.entity_id, vals.class_name, vals.char_name, to_json(vals.char_text_value) AS attr_value FROM s4_summary.char_values vals WHERE vals.class_name = '{class_name}' AND vals.char_text_value IS NOT NULL 
            UNION
            SELECT vals.entity_id, vals.class_name, vals.char_name, to_json(vals.char_integer_value) AS attr_value FROM s4_summary.char_values vals WHERE vals.class_name = '{class_name}' AND vals.char_integer_value IS NOT NULL
            UNION
            SELECT vals.entity_id, vals.class_name, vals.char_name, to_json(vals.char_decimal_value) AS attr_value FROM s4_summary.char_values vals WHERE vals.class_name = '{class_name}' AND vals.char_decimal_value IS NOT NULL
            ) 
        ON char_name IN ({quoted_char_names}) USING list(attr_value)) pv
        JOIN s4_summary.equipment_masterdata em ON em.equi_id = pv.entity_id
        ORDER BY em.functional_location; 
        """

# multiple lines of form ``to_json(pv.UNICLASS_CODE) AS json_UNICLASS_CODE,``
def _make_select_lines(columns: list) -> str:
    ans = '\n'.join(map(lambda name: f'    to_json(pv.{name}) AS json_{name},', columns))
    return ans

# line of the form: ``'UNICLASS_CODE', 'UNICLASS_DESC', 'LOCATION_ON_SITE', 'MEMO_LINE'``
def _make_quoted_name_list(columns: list) -> str:
    ans = ', '.join(map(lambda name: f'\'{name}\'', columns))
    return ans

_funcloc_masterdata_query = """
    SELECT 
        md.floc_id AS floc_id,
        md.functional_location AS functional_location,
        md.description AS description,
        md.object_type AS object_type,
        md.structure_indicator AS structure_indicator,
        md.superior_funct_loc AS superior_funct_loc,
        md.category AS category,
        md.user_status AS user_status,
        md.system_status AS system_status,
        md.installation_allowed AS installation_allowed,
        strftime(md.startup_date, '%d.%m.%Y') AS startup_date,
        lpad(CAST(md.construction_month AS TEXT), 2, '0') AS construction_month,
        md.construction_year AS construction_year,
        lpad(CAST(md.display_position AS TEXT), 4, '0') AS display_position,
        md.catalog_profile AS catalog_profile,
        md.company_code AS company_code,
        md.cost_center AS cost_center,
        md.controlling_area AS controlling_area,
        md.maintenance_plant AS maintenance_plant,
        md.main_work_center AS main_work_center,
        md.work_center AS work_center,
        md.planning_plant AS planning_plant,
        md.plant_section AS plant_section,
        md.object_number AS object_number,
        md.location AS location,
        md.address_ref AS address_ref,
    FROM s4_summary.funcloc_masterdata md 
    ORDER BY md.functional_location;
    """

_equipment_masterdata_query = """
    SELECT 
        md.equi_id AS equi_id,
        md.description AS description,
        md.functional_location AS functional_location,
        md.superord_id AS superord_id,
        md.category AS category,
        md.object_type AS object_type,
        md.user_status AS user_status,
        md.system_status AS system_status,
        strftime(md.startup_date, '%d.%m.%Y') AS startup_date,
        lpad(CAST(md.construction_month AS TEXT), 2, '0') AS construction_month,
        md.construction_year AS construction_year,
        md.manufacturer AS manufacturer,
        md.model_number AS model_number,
        md.manufact_part_number AS manufact_part_number,
        md.serial_number AS serial_number,
        md.gross_weight AS gross_weight,
        md.unit_of_weight AS unit_of_weight,
        md.technical_ident_number AS technical_ident_number,
        strftime(md.valid_from, '%d.%m.%Y') AS valid_from,
        lpad(CAST(md.display_position AS TEXT), 4, '0') AS display_position,
        md.catalog_profile AS catalog_profile,
        md.company_code AS company_code,
        md.cost_center AS cost_center,
        md.controlling_area AS controlling_area,
        md.maintenance_plant AS maintenance_plant,
        md.main_work_center AS main_work_center,
        md.work_center AS work_center,
        md.planning_plant AS planning_plant,
        md.plant_section AS plant_section,
        md.location AS location,
        md.address_ref AS address_ref,
    FROM s4_summary.equipment_masterdata md 
    ORDER BY md.functional_location;
    """