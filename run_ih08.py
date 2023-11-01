import pandas as pd
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih08.gen_sqlite import GenSqlite
from sptlibs.ih08.gen_duckdb import GenDuckdb

output_directory = 'g:/work/2023/telemetry'

    
gensqlite = GenSqlite(output_directory=output_directory)
gensqlite.add_ih08_with_aib_reference(XlsxSource('g:/work/2023/telemetry/ih08-netwtl-20231101.xlsx', 'Sheet1'), table_name='netwtl')
sqlite_path = gensqlite.gen_sqlite()

genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
genduckdb.add_s4_equipment_master_insert(sqlite_table='netwtl')
genduckdb.gen_duckdb()

