# temp_floc_builder2.py


import duckdb
import data_import.s4_ztables.duckdb_import as duckdb_import_ztables
import data_import.excel_table.duckdb_import as duckdb_import_excel

duckdb_path = 'E:/coding/work/work-sql/floc_builder/floc_builder.duckdb'
ztable_source_directory = 'g:/work/2024/asset_data_facts/s4_ztables'
worklist_path = 'g:/work/2025/floc_builder/har55_source/har55_worklist.with_notes.xlsx'
ih06_path = 'g:/work/2025/floc_builder/har55_source/HAR55_ih06_20250110095604.xlsx'

con = duckdb.connect(database=duckdb_path, read_only=False)
duckdb_import_ztables.create_duckdb_ztables(source_directory=ztable_source_directory, con=con)
duckdb_import_excel.db_import_excel_sheet(xls_path=worklist_path, sheet_name='Flocs', table_name='raw_data.worklist', con=con)
duckdb_import_excel.db_import_excel_sheet(xls_path=ih06_path, sheet_name='Sheet1', table_name='raw_data.ih06_export', con=con)
con.close()
print(f"Done - added raw data to: {duckdb_path}")

