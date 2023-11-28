import re
import pandas as pd
import sqlite3
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih06_ih08.gen_duckdb import GenDuckdb

output_directory = 'G:/work/2023/ih06_ih08'

xlsx_path = pd.ExcelFile('g:/work/2023/ih06_ih08/ih08-with-multiclasses.XLSX')
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'

genduckdb = GenDuckdb()
genduckdb.set_output_directory(output_directory=output_directory)
genduckdb.set_db_name(db_name='ih08-with-multiclasses003.duckdb')
genduckdb.add_classlist_tables(classlists_duckdb_path=classlists_db)
# genduckdb.add_ih06_export(XlsxSource('g:/work/2023/telemetry/ih06-ctos-temp.xlsx', 'Sheet1'), table_name='ctos')
genduckdb.add_ih08_export(XlsxSource(xlsx_path, 'Sheet1'))
duckdb_apth = genduckdb.gen_duckdb()

