# run_ihx_summary_report.py

import pandas as pd
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ih06_ih08.gen_duckdb import GenDuckdb
from sptlibs.ih06_ih08.gen_summary_report import GenSummaryReport

output_db_path      = 'G:/work/2023/ih06_ih08/new_ihx_summary_db.duckdb'
output_xlsx_path    = 'G:/work/2023/ih06_ih08/new_ihx_summary_report.xlsx'
ih06_xlsx_path      = pd.ExcelFile('g:/work/2023/ih06_ih08/fie08-ih06-with-class.XLSX')
ih08_xlsx_path      = pd.ExcelFile('g:/work/2023/ih06_ih08/fie08-ih08-with-class2.XLSX')
classlists_db       = 'g:/work/2023/classlist/classlists.duckdb'

gen_duckdb = GenDuckdb(classlists_duckdb_path=classlists_db, duckdb_output_path=output_db_path)
gen_duckdb.add_ih06_export(XlsxSource(ih06_xlsx_path, 'Sheet1'))
gen_duckdb.add_ih08_export(XlsxSource(ih08_xlsx_path, 'Sheet1'))
duckdb_path = gen_duckdb.gen_duckdb()

gen_summary = GenSummaryReport(db_path=duckdb_path, xlsx_output_name=output_xlsx_path)
gen_summary.gen_summary_report()



