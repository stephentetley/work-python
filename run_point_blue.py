import sqlite3 as sqlite3
import sptlibs.import_utils as import_utils


output_path = 'g:/work/2023/point_blue/point_blue_imports1.sqlite3'
worklist_source_path = 'g:/work/2023/point_blue/OP-updates-combined.xlsx'
con = sqlite3.connect(output_path)
import_utils.import_sheet(xlsx_path='g:/work/2023/point_blue/aib_point_blue_4g_export_20231011.xlsx', sheet_name='Sheet1', table_name='aib_point_blue_4g', con=con)
import_utils.import_sheet(xlsx_path='g:/work/2023/point_blue/aib_point_blue_export_20231011.xlsx', sheet_name='Sheet1', table_name='aib_point_blue', con=con)
import_utils.import_sheet(xlsx_path='g:/work/2023/point_blue/s4_point_blue_en.xlsx', sheet_name='Sheet1', table_name='s4_point_blue', con=con)
import_utils.import_sheet(xlsx_path=worklist_source_path, sheet_name='Sheet1', table_name='worklist', con=con)
import_utils.import_sheet(xlsx_path='g:/work/2023/point_blue/telemetry_fact_table.xlsx', sheet_name='Sheet1', table_name='telemetry_facts', con=con)
con.close
print(f'{output_path} created')
