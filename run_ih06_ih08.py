import re
import pandas as pd
import sqlite3
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih06_ih08.gen_duckdb import GenDuckdb

output_directory = 'G:/work/2023/ih06_ih08'

ih06_xlsx_path = pd.ExcelFile('g:/work/2023/ih06_ih08/fie08-ih06-with-class.XLSX')
ih08_xlsx_path = pd.ExcelFile('g:/work/2023/ih06_ih08/fie08-ih08-with-class2.XLSX')
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'

genduckdb = GenDuckdb()
genduckdb.set_output_directory(output_directory=output_directory)
genduckdb.set_db_name(db_name='new_summary_db.duckdb')
genduckdb.set_output_report_name(xlsx_name='new_summary_report.xlsx')
genduckdb.add_classlist_source(classlists_duckdb_path=classlists_db)
genduckdb.add_ih06_export(XlsxSource(ih06_xlsx_path, 'Sheet1'))
genduckdb.add_ih08_export(XlsxSource(ih08_xlsx_path, 'Sheet1'))
duckdb_path = genduckdb.gen_duckdb()


