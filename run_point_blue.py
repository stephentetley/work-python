# import sqlite3 as sqlite3
# import sptlibs.import_utils as import_utils
from sptlibs.xlsx_source import XlsxSource
from sptlibs.assets.gen_sqlite import GenSqlite

gensqlite = GenSqlite('g:/work/2023/point_blue')
gensqlite.db_name = 'point_blue_imports_03.sqlite3'
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/aib_point_blue_4g_export_20231011.xlsx', 'Sheet1'), table_name='aib_point_blue_4g')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/aib_point_blue_export_20231011.xlsx', 'Sheet1'), table_name='aib_point_blue')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/s4_point_blue_export_with_east_north-20231019.XLSX', 'Sheet1'), table_name='s4_point_blue')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/SR-updates-combined.xlsx', 'Sheet1'), table_name='worklist')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/telemetry_fact_table.xlsx', 'Sheet1'), table_name='telemetry_facts')
gensqlite.gen_sqlite()

# output_path = 'g:/work/2023/point_blue/point_blue_imports2.sqlite3'
# worklist_source_path = 'g:/work/2023/point_blue/SR-updates-combined.xlsx'
# con = sqlite3.connect(output_path)
# import_utils.import_sheet(XlsxSource('g:/work/2023/point_blue/aib_point_blue_4g_export_20231011.xlsx', 'Sheet1'), table_name='aib_point_blue_4g', con=con)
# import_utils.import_sheet(XlsxSource('g:/work/2023/point_blue/aib_point_blue_export_20231011.xlsx', 'Sheet1'), table_name='aib_point_blue', con=con)
# import_utils.import_sheet(XlsxSource('g:/work/2023/point_blue/s4_point_blue_export_with_east_north-20231019.XLSX', 'Sheet1'), table_name='s4_point_blue', con=con)
# import_utils.import_sheet(XlsxSource(worklist_source_path, 'Sheet1'), table_name='worklist', con=con)
# import_utils.import_sheet(XlsxSource('g:/work/2023/point_blue/telemetry_fact_table.xlsx', 'Sheet1'), table_name='telemetry_facts', con=con)
# con.close
# print(f'{output_path} created')
