
from sptlibs.xlsx_source import XlsxSource
from sptlibs.assets.gen_sqlite import GenSqlite
from sptapps.pointblue.pointblue_gen_duckdb import PointblueGenDuckdb

output_directory = 'g:/work/2023/point_blue'

gensqlite = GenSqlite(output_directory=output_directory)
gensqlite.db_name = 'assets_temp.sqlite3'
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/aib_point_blue_4g_export_20231011.xlsx', 'Sheet1'), table_name='aib_point_blue_4g')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/aib_point_blue_export_20231011.xlsx', 'Sheet1'), table_name='aib_point_blue')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/s4_point_blue_export_with_east_north-20231019.XLSX', 'Sheet1'), table_name='s4_point_blue')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/SR-updates-combined.xlsx', 'Sheet1'), table_name='aib_worklist')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/telemetry_fact_table.xlsx', 'Sheet1'), table_name='telemetry_facts')
sqlite_path = gensqlite.gen_sqlite()

genduckdb = PointblueGenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
genduckdb.db_name = 'assets.duckdb'
genduckdb.add_aib_worklist_ddl()
genduckdb.add_aib_master_data_insert(sqlite_table='aib_point_blue_4g')
genduckdb.add_aib_master_data_insert(sqlite_table='aib_point_blue')
genduckdb.add_s4_master_data_insert(sqlite_table='s4_point_blue')
genduckdb.add_aib_worklist_insert(sqlite_table='aib_worklist')
genduckdb.add_telemetry_facts_insert(sqlite_table='telemetry_facts')
duckdb_path = genduckdb.gen_duckdb()

