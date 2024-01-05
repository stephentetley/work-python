import re
import pandas as pd
import sqlite3
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih06_ih08.gen_duckdb import GenDuckdb
from sptlibs.ih06_ih08.gen_summary_report import GenSummaryReport

output_directory = 'G:/work/2023/ih06_ih08'
output_report = 'G:/work/2023/ih06_ih08/new_ihx_summary_report.xlsx'
ih06_xlsx_path = pd.ExcelFile('g:/work/2023/ih06_ih08/fie08-ih06-with-class.XLSX')
ih08_xlsx_path = pd.ExcelFile('g:/work/2023/ih06_ih08/fie08-ih08-with-class2.XLSX')
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'

gen_duckdb = GenDuckdb()
gen_duckdb.set_output_directory(output_directory=output_directory)
gen_duckdb.set_db_name(db_name='new_ihx_summary_db.duckdb')
gen_duckdb.add_classlist_source(classlists_duckdb_path=classlists_db)
gen_duckdb.add_ih06_export(XlsxSource(ih06_xlsx_path, 'Sheet1'))
gen_duckdb.add_ih08_export(XlsxSource(ih08_xlsx_path, 'Sheet1'))
duckdb_path = gen_duckdb.gen_duckdb()

gen_summary = GenSummaryReport(db_path=duckdb_path, xlsx_output_name=output_report)
gen_summary.gen_summary_report()



