import pandas as pd
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.assets.gen_sqlite import GenSqlite
from sptapps.pointblue.pointblue_gen_duckdb import PointblueGenDuckdb
import sptapps.pointblue.pointblue_retire_report as pointblue_retire_report
import sptapps.pointblue.pointblue_new_4g_report as pointblue_new_4g_report

output_directory = 'g:/work/2023/point_blue'


def remove_requested_by(df: pd.DataFrame) -> pd.DataFrame:
    if 'Requested By' in df.columns:
        return df.drop(columns=['Requested By'])
    else: 
        return df
    
gensqlite = GenSqlite(output_directory=output_directory)
gensqlite.db_name = 'assets_ar.sqlite3'
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/aib_point_blue_4g_export_20231011.xlsx', 'Sheet1'), table_name='aib_point_blue_4g', df_trafo=None)
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/aib_point_blue_export_20231011.xlsx', 'Sheet1'), table_name='aib_point_blue', df_trafo=None)
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/s4_point_blue_export_with_east_north-20231019.XLSX', 'Sheet1'), table_name='s4_point_blue_equi', df_trafo=None)
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/AR-updates-combined.xlsx', 'Sheet1'), table_name='aib_worklist', df_trafo=remove_requested_by)
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/point_blue/telemetry_fact_table.xlsx', 'Sheet1'), table_name='telemetry_facts', df_trafo=None)
sqlite_path = gensqlite.gen_sqlite()

genduckdb = PointblueGenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
genduckdb.db_name = 'assets-ar.duckdb'
genduckdb.add_aib_worklist_ddl()
genduckdb.add_aib_master_data_insert(sqlite_table='aib_point_blue_4g')
genduckdb.add_aib_master_data_insert(sqlite_table='aib_point_blue')
genduckdb.add_s4_equipment_master_insert(sqlite_table='s4_point_blue_equi')
genduckdb.add_aib_worklist_insert(sqlite_table='aib_worklist')
genduckdb.add_telemetry_facts_insert(sqlite_table='telemetry_facts')
genduckdb.add_ai2_aib_reference_insert(sqlite_table='s4_point_blue')
genduckdb.add_easting_northing_insert(sqlite_table='s4_point_blue')
duckdb_path = genduckdb.gen_duckdb()

retire_csv_outpath='g:/work/2023/point_blue/pointblue_retire_report_ar.csv'
pointblue_retire_report.output_retire_report(duckdb_path=duckdb_path, csv_outpath=retire_csv_outpath)
print(f'Wrote {retire_csv_outpath}')

new4g_csv_outpath='g:/work/2023/point_blue/pointblue_new4g_report_ar.csv'
pointblue_new_4g_report.output_new_4g_report(duckdb_path=duckdb_path, csv_outpath=new4g_csv_outpath)
print(f'Wrote {new4g_csv_outpath}')
