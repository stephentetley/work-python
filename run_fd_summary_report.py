# run_fd_summary_report.py

from sptlibs.file_download.gen_duckdb import GenDuckdb
from sptlibs.file_download.gen_summary_report import GenSummaryReport

source_directory    = 'g:/work/2024/cpumps/batch2'
glob_pattern        = '*download.txt'
classlists_db       = 'g:/work/2024/classlists/classlists.duckdb'
duckdb_output_path  = 'g:/work/2024/cpumps/batch2/cp_batch2_data.duckdb'
xlsx_output_path    = 'g:/work/2024/cpumps/batch2/cp-batch2-summary-report.xlsx'


gen_duckdb = GenDuckdb(classlists_duckdb_path=classlists_db, duckdb_output_path=duckdb_output_path)
gen_duckdb.add_downloads_source_directory(source_dir=source_directory, glob_pattern=glob_pattern)
db = gen_duckdb.gen_duckdb()

gen_summary = GenSummaryReport(db_path=duckdb_output_path, xlsx_output_name=xlsx_output_path)
gen_summary.gen_summary_report()


