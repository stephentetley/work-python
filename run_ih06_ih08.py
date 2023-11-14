import re
import pandas as pd
import sqlite3
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih06_ih08.gen_sqlite import GenSqlite
from sptlibs.ih06_ih08.gen_duckdb2 import GenDuckdb2
from sptlibs.ih06_ih08.column_range import ColumnRange
import sptlibs.ih06_ih08.transform_xlsx as transform_xlsx
import sptlibs.import_utils as import_utils

output_directory = 'G:/work/2023/ih06_ih08'

xlsx_path = pd.ExcelFile('g:/work/2023/ih06_ih08/ih08-with-multiclasses.XLSX')

# Go straight to duckdb...
# gensqlite = GenSqlite(output_directory=output_directory)
# gensqlite.set_db_name(db_name='ih08-with-multiclasses.sqlite3')
# # gensqlite.add_ih06_export(XlsxSource('g:/work/2023/telemetry/ih06-ctos-temp.xlsx', 'Sheet1'), table_name='ctos')
# gensqlite.add_ih08_export(XlsxSource(xlsx_path, 'Sheet1'))
# sqlite_path = gensqlite.gen_sqlite()

genduckdb = GenDuckdb2(output_directory=output_directory)
genduckdb.set_db_name(db_name='ih08-with-multiclasses.duckdb')
# genduckdb.add_ih06_export(XlsxSource('g:/work/2023/telemetry/ih06-ctos-temp.xlsx', 'Sheet1'), table_name='ctos')
genduckdb.add_ih08_export(XlsxSource(xlsx_path, 'Sheet1'))
duckdb_apth = genduckdb.gen_duckdb()

