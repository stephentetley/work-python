# run_file_downloads.py

import os
import sptlibs.file_download.file_download_parser as file_download_parser
from sptlibs.file_download.gen_sqlite import GenSqlite
from sptlibs.file_download.gen_duckdb import GenDuckdb

equi1 = 'g:/work/2023/file_download/equi-class-chars-sample/equi_download.txt'
classequi1 = 'g:/work/2023/file_download/equi-class-chars-sample/classequi_download.txt'
valuaequi1 = 'g:/work/2023/file_download/equi-class-chars-sample/valuaequi_download.txt'
output_directory = 'g:/work/2023/file_download/equi-class-chars-sample'

if os.path.exists(output_directory): 
    print(f'Found: {output_directory}')

    # TODO - custom functions for entity_types?
    gensqlite = GenSqlite(output_directory=output_directory)
    gensqlite.db_name = 'file_download_equi3.sqlite3'
    gensqlite.add_file_download(equi1, table_name='equi', df_trafo=None)
    gensqlite.add_file_download(classequi1, table_name='classequi', df_trafo=None)
    gensqlite.add_file_download(valuaequi1, table_name='valuaequi', df_trafo=None)
    sqlite_path = gensqlite.gen_sqlite()

    genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
    genduckdb.gen_duckdb()

# Note - file downloads best used for checking so we need most of the information in equi downloads.
# Still need to know char_type to choose which field to look at for value in the valuaequi field.
