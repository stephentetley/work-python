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

from typing import Callable, Any
import duckdb
import polars as pl
import xlsxwriter
from xlsxwriter import Workbook
from sptlibs.utils.polars_xlsx_table import PolarsXlsxTable
from sptlibs.utils.sql_script_runner import SqlScriptRunner

def gen_report(*, xls_output_path: str, con: duckdb.DuckDBPyConnection) -> None:
    with xlsxwriter.Workbook(xls_output_path) as workbook:
        _add_table(
            select_query=_floc_master_data, 
            sheet_name='functional_location',
            column_formats={'construction_year': 'General', 
                            'company_code': 'General', 
                            'cost_center': 'General', 
                            'controlling_area': 'General', 
                            'maintenance_plant': 'General', 
                            'planning_plant': 'General', 
                            'address_ref': 'General'},
            con=con, workbook=workbook)
        
        _add_table(
            select_query=_equi_master_data, 
            sheet_name='equipment',
            column_formats={'construction_year': 'General', 
                            'company_code': 'General', 
                            'cost_center': 'General', 
                            'controlling_area': 'General', 
                            'maintenance_plant': 'General', 
                            'planning_plant': 'General', 
                            'address_ref': 'General'},
            con=con, workbook=workbook)
        
        _add_table(
            select_query=_flocsummary_aib_reference,
            sheet_name='f.aib_reference', 
            column_formats = {},
            con=con, workbook=workbook)
        
        _add_table(
            select_query=_flocsummary_east_north,
            sheet_name='f.east_north', 
            column_formats = _general_columns(['easting', 'northing']),
            con=con, workbook=workbook)
        
        _add_table(
            select_query=_flocsummary_solution_id,
            sheet_name='f.solution_id', 
            column_formats = {},
            con=con, workbook=workbook)
        
        _add_flocclass_tables(con=con, workbook=workbook)
        
        _add_table(
            select_query=_equisummary_aib_reference,
            sheet_name='e.aib_reference', 
            column_formats = {},
            con=con, workbook=workbook)
        
        _add_table(
            select_query=_equisummary_asset_condition,
            sheet_name='e.asset_condition', 
            column_formats = _general_columns(['survey_date']),
            con=con, workbook=workbook)
        
        _add_table(
            select_query=_equisummary_east_north,
            sheet_name='e.east_north', 
            column_formats = _general_columns(['easting', 'northing']),
            con=con, workbook=workbook)
        
        _add_table(
            select_query=_equisummary_solution_id,
            sheet_name='e.solution_id', 
            column_formats = {},
            con=con, workbook=workbook)
        
        _add_equiclass_tables(con=con, workbook=workbook)
        

def _general_columns(ls: list[str]) -> dict[str, str]:
    return {key: 'General' for key in ls}

def _add_table(
        *, 
        select_query: str, 
        workbook: Workbook, 
        sheet_name: str,
        column_formats: dict[str, str],
        con: duckdb.DuckDBPyConnection) -> None:
    df = con.execute(query=select_query).pl()
    PolarsXlsxTable(df=df).write_excel(
            workbook=workbook, sheet_name=sheet_name, 
            column_formats = column_formats)

_floc_master_data = """
SELECT 
    t.* REPLACE (
        format('{:02d}', construction_month) AS construction_month, 
        format('{:04d}', display_position) AS display_position,
        strftime(startup_date, '%d.%m.%Y') AS startup_date),
FROM s4_class_rep.floc_master_data t
ORDER BY t.functional_location;
"""

_equi_master_data = """
SELECT 
    t.* REPLACE (
        format('{:02d}', construction_month) AS construction_month, 
        format('{:04d}', display_position) AS display_position,
        strftime(startup_date, '%d.%m.%Y') AS startup_date, 
        strftime(valid_from, '%d.%m.%Y') AS valid_from),
FROM s4_class_rep.equi_master_data t
ORDER BY t.equipment_id;
"""


_flocsummary_aib_reference = """
SELECT 
    t.*,
FROM s4_class_rep.vw_flocsummary_aib_reference t
ORDER BY t.functional_location;
"""


_equisummary_aib_reference = """
SELECT 
    t.*,
FROM s4_class_rep.vw_equisummary_aib_reference t
ORDER BY t.equipment_id;
"""


_equisummary_asset_condition = """
SELECT 
    t.* REPLACE (strftime(last_refurbished_date, '%d.%m.%Y') AS last_refurbished_date),
FROM s4_class_rep.vw_equisummary_asset_condition t
ORDER BY t.equipment_id;
"""



_flocsummary_east_north = """
SELECT 
    t.*,
FROM s4_class_rep.vw_flocsummary_east_north t
ORDER BY t.functional_location;
"""

_equisummary_east_north = """
SELECT 
    t.*,
FROM s4_class_rep.vw_equisummary_east_north t
ORDER BY t.equipment_id;
"""

_flocsummary_solution_id = """
SELECT 
    t.*,
FROM s4_class_rep.vw_flocsummary_solution_id t
ORDER BY t.functional_location;
"""

_equisummary_solution_id = """
SELECT 
    t.*,
FROM s4_class_rep.vw_equisummary_solution_id t
ORDER BY t.equipment_id;
"""

def _add_flocclass_tables(
        *, 
        workbook: Workbook, 
        con: duckdb.DuckDBPyConnection) -> None:
    def action(row: dict[str, Any], df: pl.DataFrame) -> None: 
        sheet_name = "f.{}".format(row.get('class_name', "unknown"))
        PolarsXlsxTable(df=df).write_excel(
            workbook=workbook, sheet_name=sheet_name, 
            column_formats = {})
    runner = SqlScriptRunner(None, con=con)
    runner.eval_sql_generating_stmt(sql_query=_get_flocclass_tables, action=action)


_get_flocclass_tables = """
WITH cte AS (
    SELECT 
        t.class_name,
    FROM s4_class_rep.vw_flocclass_stats t 
    WHERE t.estimated_size > 0
)
SELECT 
    t.class_name AS class_name,
    format(E'SELECT t.* FROM s4_class_rep.vw_flocsummary_{} t ORDER BY t.functional_location;', t.class_name) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;
"""

def _add_equiclass_tables(
        *, 
        workbook: Workbook, 
        con: duckdb.DuckDBPyConnection) -> None:
    def action(row: dict[str, Any], df: pl.DataFrame) -> None: 
        sheet_name = "e.{}".format(row.get('class_name', "unknown"))
        PolarsXlsxTable(df=df).write_excel(
            workbook=workbook, sheet_name=sheet_name, 
            column_formats = {})
    runner = SqlScriptRunner(None, con=con)
    runner.eval_sql_generating_stmt(sql_query=_get_equiclass_tables, action=action)
    
    
_get_equiclass_tables = """
WITH cte AS (
    SELECT 
        t.class_name,
    FROM s4_class_rep.vw_equiclass_stats t 
    WHERE t.estimated_size > 0
)
SELECT 
    t.class_name AS class_name,
    format(E'SELECT t.* FROM s4_class_rep.vw_equisummary_{} t ORDER BY t.equipment_id;', t.class_name) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;
"""

