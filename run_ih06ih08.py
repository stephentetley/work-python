import pandas as pd
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih06ih08.gen_sqlite import GenSqlite
from sptlibs.ih06ih08.gen_duckdb import GenDuckdb

output_directory = 'g:/work/2023/telemetry'

    
gensqlite = GenSqlite(output_directory=output_directory)
gensqlite.add_ih06_export(XlsxSource('g:/work/2023/telemetry/ih06-ctos-temp.xlsx', 'Sheet1'), table_name='ctos')
gensqlite.add_ih08_export(XlsxSource('g:/work/2023/telemetry/ih08-netwtl-20231102.xlsx', 'Sheet1'), table_name='netwtl')
sqlite_path = gensqlite.gen_sqlite()

genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
genduckdb.add_s4_equipment_master_insert(sqlite_table='netwtl', has_aib_characteritics=True)
genduckdb.add_s4_funcloc_master_insert(sqlite_table='ctos', has_aib_characteritics=False)
genduckdb.gen_duckdb()

