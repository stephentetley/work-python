import re
import pandas as pd
import sqlite3
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih06_ih08.gen_sqlite import GenSqlite
from sptlibs.ih06_ih08.gen_duckdb import GenDuckdb
from sptlibs.ih06_ih08.column_range import ColumnRange
import sptlibs.ih06_ih08.transform_xlsx as transform_xlsx
import sptlibs.import_utils as import_utils

output_directory = 'G:/work/2023/ih06_ih08'

xlsx_path = pd.ExcelFile('g:/work/2023/ih06_ih08/ih08-with-multiclasses.XLSX')
    
gensqlite = GenSqlite(output_directory=output_directory)
gensqlite.set_db_name(db_name='ih08-with-multiclasses.sqlite3')
# gensqlite.add_ih06_export(XlsxSource('g:/work/2023/telemetry/ih06-ctos-temp.xlsx', 'Sheet1'), table_name='ctos')
gensqlite.add_ih08_export(XlsxSource(xlsx_path, 'Sheet1'))
sqlite_path = gensqlite.gen_sqlite()

# genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
# genduckdb.add_s4_equipment_master_insert(sqlite_table='ih08', has_aib_characteritics=True)
# # genduckdb.add_s4_funcloc_master_insert(sqlite_table='ctos', has_aib_characteritics=False)
# genduckdb.gen_duckdb()

