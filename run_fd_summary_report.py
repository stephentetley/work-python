# run_fd_summary_report.py




import duckdb
import sptlibs.cmdline_utils as cmdline_utils
import sptlibs.data_import.file_download.duckdb_import as duckdb_import
import sptlibs.data_import.classlists.duckdb_import as classlists_duckdb_import
from sptlibs.reports.file_download_summary.gen_summary_report import GenSummaryReport

# Set up path to config file if not done already...
# $env:ASSET_DATA_CONFIG_DIR = 'e:\\coding\\work\\work-python\\config' 

config = cmdline_utils.get_asset_data_config().get('file_download_summary', None)



glob_pattern        = config.get('glob_pattern', '*download.txt')
classlists_db       = 'g:/work/2024/asset_data_facts/s4_classlists/classlists2.duckdb'
source_directory    = 'g:/work/2024/point_blue/pb04'
duckdb_output_path  = source_directory + '/fd_summary_data.duckdb'
xlsx_output_path    = source_directory + '/pb04-fd-summary-report.xlsx'


conn = duckdb.connect(database=duckdb_output_path)

duckdb_import.init(con=conn)
duckdb_import.store_download_files(source_dir=source_directory, glob_pattern=glob_pattern, con=conn)
classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)
conn.close()

gen_summary = GenSummaryReport(db_path=duckdb_output_path, xlsx_output_name=xlsx_output_path)
gen_summary.gen_summary_report()


