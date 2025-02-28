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
import polars as pl
import xlsxwriter

def output_csv_report(*, duckdb_path: str, select_stmt: str, csv_outpath: str) -> str:
    '''`select_stmt` should not be terminated with a semicolon'''
    copy_stmt = f"""
        COPY (
            {select_stmt}
        ) TO '{csv_outpath}' (FORMAT CSV, DELIMITER ',', HEADER)
        """
    try: 
        con = duckdb.connect(duckdb_path)
        con.sql(copy_stmt)
        con.close()
    except Exception as exn:
        print(exn)
        print(copy_stmt)
    


yellow_bold_header_format={
    'bold': True,
    'text_wrap': False,
    'align': 'left',
    'fg_color': '#FFFF00',
    'border': None}, 


_equisummary_aib_reference = """
SELECT 
    t.*,
FROM s4_class_rep.vw_equisummary_aib_reference t
ORDER BY t.equipment_id;
"""

def write_sql_table_to_excel(*, 
                             qualified_table_name: str,
                             con: duckdb.DuckDBPyConnection,
                             workbook: xlsxwriter.Workbook, 
                             sheet_name: str, 
                             column_formats: dict = None, 
                             header_format: dict = None,
                             order_by_columns: list = []) -> None:
    orderby_clause = "" if not order_by_columns else "ORDER BY " + ", ".join(order_by_columns)
    query = f"""
        SELECT 
            *,
        FROM {qualified_table_name}
        {orderby_clause};
    """
    write_sql_query_to_excel(select_query=query, 
                             con=con, 
                             workbook=workbook,
                             sheet_name=sheet_name,
                             column_formats=column_formats,
                             header_format=header_format)

def write_sql_query_to_excel(*, 
                             select_query: str,
                             con: duckdb.DuckDBPyConnection,
                             workbook: xlsxwriter.Workbook, 
                             sheet_name: str, 
                             column_formats: dict = None, 
                             header_format: dict = None) -> None:
    df = con.execute(query=select_query).pl()
    write_pl_dataframe_to_excel(df=df,
                                workbook= workbook,
                                sheet_name=sheet_name, 
                                column_formats = column_formats)

def write_pl_dataframe_to_excel(*, 
                                df: pl.DataFrame, 
                                workbook: xlsxwriter.Workbook, 
                                sheet_name: str, 
                                column_formats: dict = None, 
                                header_format: dict = None) -> None:
    if not header_format:
        header_format = yellow_bold_header_format
    if not column_formats:
        column_formats = {}
    df.write_excel(
        workbook, sheet_name,
        header_format={
            'bold': True,
            'text_wrap': False,
            'align': 'left',
            'fg_color': '#FFFF00',
            'border': None}, 
        freeze_panes=(1, 0),
        autofit=True, 
        column_formats = column_formats)