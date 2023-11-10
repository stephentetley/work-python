# run_summary_report2.py

import os
import duckdb
import pandas as pd
from sptlibs.file_download.gen_sqlite import GenSqlite
from sptlibs.file_download.gen_duckdb import GenDuckdb
import sptapps.download_summary.duckdb_queries as duckdb_queries
import sptapps.download_summary.duckdb_setup as duckdb_setup
import sptapps.download_summary.df_transforms as df_transforms
import sptapps.download_summary.make_summary_report as make_summary_report

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
    # pandas looks best fit...
    con = duckdb.connect(duckdb_path)
    con.execute(duckdb_setup.vw_characteristics_summary_ddl)
    con.execute(duckdb_queries.equi_summary_report)
    df = con.df()
    print(df)
    print(df.dtypes)
    with pd.ExcelWriter(output_xls) as xlwriter: 
        df1 = df_transforms.equipment_rewrite_equi_classes(df)
        df1.to_excel(xlwriter, engine='xlsxwriter', sheet_name='equipment_master')
        # tabs
        con.execute(duckdb_queries.get_classes_used_query)
        for (class_type, class_name) in con.fetchall():
            tab_name = make_summary_report.make_class_tab_name(class_type=class_type, class_name=class_name)
            print(tab_name)
            con.execute(query= duckdb_queries.class_tab_summary_report, parameters={'class_type': class_type, 'class_name': class_name})
            df2 = con.df()
            df3 = df_transforms.class_char_rewrite_characteristics(df2)
            df3.to_excel(xlwriter, engine='xlsxwriter', sheet_name=tab_name)
        con.close()
