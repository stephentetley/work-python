# run_fd_summary_report.py

from sptlibs.data_import.file_download.gen_duckdb import GenDuckdb
from sptlibs.data_import.file_download.gen_summary_report import GenSummaryReport

glob_pattern        = '*download.txt'
classlists_db       = 'g:/work/2024/classlists/classlists2.duckdb'
source_directory    = 'g:/work/2024/lstnut/file_download1'
duckdb_output_path  = source_directory + '/lstnut_fd_data.duckdb'
xlsx_output_path    = source_directory + '/lstnut_fd-summary-report.xlsx'


gen_duckdb = GenDuckdb(classlists_duckdb_path=classlists_db, duckdb_output_path=duckdb_output_path)
gen_duckdb.add_downloads_source_directory(source_dir=source_directory, glob_pattern=glob_pattern)
db = gen_duckdb.gen_duckdb()

gen_summary = GenSummaryReport(db_path=duckdb_output_path, xlsx_output_name=xlsx_output_path)
gen_summary.gen_summary_report()


