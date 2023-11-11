# run_summary_report.py

from sptapps.download_summary.gen_summary_report import GenSummaryReport

source_directory1 = 'G:/work/2023/file_download/hs-mere-floc-equi'
glob_pattern = 'hs*.txt'
output_directory = 'g:/work/2023/file_download/hs-mere-floc-equi'
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'

genreport = GenSummaryReport()
genreport.set_output_directory(output_directory=output_directory)
genreport.set_classlists_db_path(classlists_db_path=classlists_db)
genreport.add_downloads_source_directory(src_dir=source_directory1, glob_pattern=glob_pattern)
genreport.set_sqlite_output_name(sqlite_output_name='fd1.sqlite3')
genreport.set_duckdb_output_name(duckdb_output_name='fd2.duckdb')
genreport.set_output_report_name(xlsx_name='hs-mere-summary.xlsx')
xlsx = genreport.gen_summary_report()
print(f'{xlsx} created')


