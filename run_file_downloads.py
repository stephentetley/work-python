# run_file_downloads.py

import os
from sptlibs.file_download.gen_sqlite import GenSqlite
from sptlibs.file_download.gen_duckdb import GenDuckdb

source_directories = ['g:/work/2023/file_download/equi-class-chars-sample',
                        'g:/work/2023/file_download/floc-class-chars-sample']
glob_pattern = '*download.txt'
output_directory = 'g:/work/2023/file_download'
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'



if os.path.exists(output_directory): 
    print(f'Found: {output_directory}')

    gensqlite = GenSqlite(output_directory=output_directory)
    for dir in source_directories: 
        gensqlite.add_file_downloads_in_directory(path=dir, glob_pattern=glob_pattern)
    gensqlite.db_name = 'file_download_equi_floc2.sqlite3'
    sqlite_path = gensqlite.gen_sqlite()

    genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
    genduckdb.db_name = 'file_download_equi_floc2.duckdb'
    genduckdb.add_classlist_tables(classlists_duckdb_path=classlists_db)
    genduckdb.gen_duckdb()

# Note - file downloads best used for checking so we need most of the information in equi downloads.
# Still need to know char_type to choose which field to look at for value in the valuaequi field.
