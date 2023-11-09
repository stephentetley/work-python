# run_summary_report2.py

import os
import duckdb
from sptlibs.file_download.gen_sqlite import GenSqlite
from sptlibs.file_download.gen_duckdb import GenDuckdb
import sptapps.download_summary.equi_summary_report as equi_summary_report

source_directories = ['G:/work/2023/file_download/new-pb4g-to-check-batch02']
glob_pattern = '*download.txt'
output_directory = 'g:/work/2023/file_download/new-pb4g-to-check-batch02'
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'
output_xls = os.path.normpath(os.path.join(output_directory, 'summary.xlsx'))



if os.path.exists(output_directory): 
    print(f'Found: {output_directory}')

    gensqlite = GenSqlite(output_directory=output_directory)
    for dir in source_directories: 
        gensqlite.add_file_downloads_in_directory(path=dir, glob_pattern=glob_pattern)
    gensqlite.db_name = 'pb4g_batch02.sqlite3'
    sqlite_path = gensqlite.gen_sqlite()

    genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
    genduckdb.db_name = 'pb4g_batch02.duckdb'
    genduckdb.add_classlist_tables(classlists_duckdb_path=classlists_db)
    duckdb_path = genduckdb.gen_duckdb()

    # Output...
    # panads looks best fit...
    con = duckdb.connect(duckdb_path)
    df = con.sql(equi_summary_report.equi_summary_report).df()
    print(df)
    print(df.dtypes)
    df.to_excel(output_xls, engine='xlsxwriter')
    con.close()
