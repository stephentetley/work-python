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

from typing import Any
import duckdb
import polars as pl
import xlsxwriter
import sptlibs.utils.export_utils as export_utils
import sptlibs.asset_schema.udfs.setup_sql_udfs as setup_sql_udfs
import sptlibs.data_access.s4_classlists.s4_classlists_import as s4_classlists_import
import sptlibs.data_access.file_download.file_download_import as file_download_import
import sptlibs.asset_schema.file_download_to_s4_classrep.file_download_to_s4_classrep as file_download_to_s4_classrep
from sptlibs.utils.sql_script_runner import SqlScriptRunner
import sptlibs.asset_checking.asset_checking as asset_checking

def duckdb_init(*, 
                file_download_files: list[str],
                classlists_db_path: str,
                con: duckdb.DuckDBPyConnection) -> None: 
    setup_sql_udfs.setup_udfx_macros(con=con)
    file_download_import.duckdb_init(con=con)
    file_download_import.duckdb_import_files(file_paths=file_download_files, con=con)
    file_download_to_s4_classrep.translate_file_download_to_s4_classrep(s4_classlists_db_path=classlists_db_path,
                                                                        con=con)
    asset_checking.setup_asset_checking(con=con)
    asset_checking.run_s4_classrep_checkers(con=con)
    asset_checking.run_s4_classrep_file_download_checkers(con=con)



def gen_xls_report(*, xls_output_path: str, con: duckdb.DuckDBPyConnection) -> None:
    with xlsxwriter.Workbook(xls_output_path) as workbook:
        export_utils.write_sql_query_to_excel(
            select_query="""
                SELECT 
                    t.* REPLACE (
                        format('{:02d}', construction_month) AS construction_month, 
                        format('{:04d}', display_position) AS display_position,
                        strftime(startup_date, '%d.%m.%Y') AS startup_date),
                FROM s4_classrep.floc_masterdata t
                ORDER BY t.functional_location;
                """,
            con=con,
            workbook=workbook,
            sheet_name='functional_location',
            column_formats={'construction_year': 'General', 
                            'company_code': 'General', 
                            'cost_center': 'General', 
                            'controlling_area': 'General', 
                            'maintenance_plant': 'General', 
                            'planning_plant': 'General', 
                            'address_ref': 'General'})

        export_utils.write_sql_query_to_excel(
            select_query="""
                SELECT 
                    t.* REPLACE (
                        format('{:02d}', construction_month) AS construction_month, 
                        format('{:04d}', display_position) AS display_position,
                        strftime(startup_date, '%d.%m.%Y') AS startup_date, 
                        strftime(valid_from, '%d.%m.%Y') AS valid_from),
                FROM s4_classrep.equi_masterdata t
                ORDER BY t.equipment_id;
                """,
            sheet_name='equipment',
            column_formats={'construction_year': 'General', 
                            'company_code': 'General', 
                            'cost_center': 'General', 
                            'controlling_area': 'General', 
                            'maintenance_plant': 'General', 
                            'planning_plant': 'General', 
                            'address_ref': 'General'},
            con=con, workbook=workbook)
        
        export_utils.write_sql_table_to_excel(
            qualified_table_name='s4_classrep.vw_flocsummary_aib_reference',
            order_by_columns=['functional_location'],
            sheet_name='f.aib_reference', 
            column_formats = {},
            con=con, workbook=workbook)
        
        export_utils.write_sql_table_to_excel(
            qualified_table_name='s4_classrep.vw_flocsummary_east_north',
            order_by_columns=['functional_location'],
            sheet_name='f.east_north', 
            column_formats = _general_columns(['easting', 'northing']),
            con=con, workbook=workbook)
        

        export_utils.write_sql_table_to_excel(
            qualified_table_name='s4_classrep.vw_flocsummary_solution_id',
            order_by_columns=['functional_location'],
            sheet_name='f.solution_id', 
            column_formats = {},
            con=con, workbook=workbook)
        
        _add_flocclass_tables(con=con, workbook=workbook)
        
        export_utils.write_sql_table_to_excel(
            qualified_table_name='s4_classrep.vw_equisummary_aib_reference',
            order_by_columns=['equipment_id'],
            sheet_name='e.aib_reference', 
            column_formats = {},
            con=con, workbook=workbook)

        export_utils.write_sql_query_to_excel(
            select_query="""
                SELECT 
                    t.* REPLACE (strftime(last_refurbished_date, '%d.%m.%Y') AS last_refurbished_date),
                FROM s4_classrep.vw_equisummary_asset_condition t
                ORDER BY t.equipment_id;
                """,
            sheet_name='e.asset_condition', 
            column_formats = _general_columns(['survey_date']),
            con=con, workbook=workbook)
        
        export_utils.write_sql_table_to_excel(
            qualified_table_name='s4_classrep.vw_equisummary_east_north',
            order_by_columns=['equipment_id'],
            sheet_name='e.east_north', 
            column_formats = _general_columns(['easting', 'northing']),
            con=con, workbook=workbook)
        
        export_utils.write_sql_table_to_excel(
            qualified_table_name='s4_classrep.vw_equisummary_solution_id',
            order_by_columns=['equipment_id'],
            sheet_name='e.solution_id', 
            column_formats = {},
            con=con, workbook=workbook)
        
        _add_equiclass_tables(con=con, workbook=workbook)
        _add_equishape_tables(con=con, workbook=workbook)
                
        export_utils.write_sql_table_to_excel(
            qualified_table_name='asset_checking.vw_checking_report',
            order_by_columns=['category', 'checker_name'],
            sheet_name='asset_checks', 
            column_formats = {},
            con=con, workbook=workbook)

def _general_columns(ls: list[str]) -> dict[str, str]:
    return {key: 'General' for key in ls}


def _add_flocclass_tables(
        *, 
        workbook: xlsxwriter.Workbook, 
        con: duckdb.DuckDBPyConnection) -> None:
    def action(row: dict[str, Any], df: pl.DataFrame) -> None: 
        sheet_name = "f.{}".format(row.get('class_name', "unknown"))
        export_utils.write_pl_dataframe_to_excel(df=df,
                                                 workbook=workbook,
                                                 sheet_name=sheet_name, 
                                                 column_formats = {})
    runner = SqlScriptRunner(None, con=con)
    runner.exec_sql_generating_stmt_with_action(sql_query=_get_flocclass_tables, action=action)


_get_flocclass_tables = """
WITH cte AS (
    SELECT 
        t.table_name,
        t.class_name,
    FROM s4_classrep.vw_flocclass_stats t 
    WHERE t.estimated_size > 0
)
SELECT 
    t.class_name AS class_name,
    format(E'SELECT * FROM simple_floc_summary(''s4_classrep.{}'') ORDER BY functional_location;', t.table_name) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;
"""

# equiclass
def _add_equiclass_tables(
        *, 
        workbook: xlsxwriter.Workbook, 
        con: duckdb.DuckDBPyConnection) -> None:
    def action(row: dict[str, Any], df: pl.DataFrame) -> None: 
        sheet_name = "e.{}".format(row.get('class_name', "unknown"))
        export_utils.write_pl_dataframe_to_excel(df=df,
                                                 workbook=workbook, 
                                                 sheet_name=sheet_name, 
                                                 column_formats = {})
    runner = SqlScriptRunner(None, con=con)
    runner.exec_sql_generating_stmt_with_action(sql_query=_get_equiclass_tables, action=action)
    
    
_get_equiclass_tables = """
WITH cte AS (
    SELECT 
        t.table_name,
        t.class_name,
    FROM s4_classrep.vw_equiclass_stats t 
    WHERE t.estimated_size > 0
)
SELECT 
    t.class_name AS class_name,
    format(E'SELECT * FROM simple_equi_summary(''s4_classrep.{}'') ORDER BY equipment_id;', t.table_name) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;
"""

# equishape
def _add_equishape_tables(
        *, 
        workbook: xlsxwriter.Workbook, 
        con: duckdb.DuckDBPyConnection) -> None:
    def action(row: dict[str, Any], df: pl.DataFrame) -> None: 
        sheet_name = "e.{}".format(row.get('class_name', "unknown"))
        export_utils.write_pl_dataframe_to_excel(df=df,
                                                 workbook=workbook, 
                                                 sheet_name=sheet_name, 
                                                 column_formats = {})
    runner = SqlScriptRunner(None, con=con)
    runner.exec_sql_generating_stmt_with_action(sql_query=_get_equishape_tables, action=action)
    
    
_get_equishape_tables = """
WITH cte AS (
    SELECT 
        t.class_name,
        t.table_name,
    FROM s4_classrep.vw_equishape_stats t 
    WHERE t.estimated_size > 0
)
SELECT 
    t.class_name AS class_name,
    format(E'SELECT * FROM simple_equi_summary(''s4_classrep.{}'') ORDER BY equipment_id;', t.table_name) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;
"""