# run_file_downloads.py

import os
import sptlibs.file_download.file_download_parser as file_download_parser
from sptlibs.file_download.gen_sqlite import GenSqlite
from sptlibs.file_download.gen_duckdb import GenDuckdb

equi1 = 'g:/work/2023/file_download/equi-class-chars-sample/equi_download.txt'
classequi1 = 'g:/work/2023/file_download/equi-class-chars-sample/classequi_download.txt'
valuaequi1 = 'g:/work/2023/file_download/equi-class-chars-sample/valuaequi_download.txt'
floc1 = 'g:/work/2023/file_download/floc-class-chars-sample/funcloc_download.txt'
classfloc1 = 'g:/work/2023/file_download/floc-class-chars-sample/classfloc_download.txt'
valuafloc1 = 'g:/work/2023/file_download/floc-class-chars-sample/valuafloc_download.txt'
output_directory = 'g:/work/2023/file_download'
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'

if os.path.exists(output_directory): 
    print(f'Found: {output_directory}')

    # TODO - custom functions for entity_types?
    gensqlite = GenSqlite(output_directory=output_directory)
    gensqlite.db_name = 'file_download_equi_floc.sqlite3'
    gensqlite.add_file_download(equi1, table_name='equi')
    gensqlite.add_file_download(classequi1, table_name='classequi')
    gensqlite.add_file_download(valuaequi1, table_name='valuaequi')
    gensqlite.add_file_download(floc1, table_name='funcloc')
    gensqlite.add_file_download(classfloc1, table_name='classfloc')
    gensqlite.add_file_download(valuafloc1, table_name='valuafloc')
    sqlite_path = gensqlite.gen_sqlite()

    genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
    genduckdb.add_funcloc_table(sqlite_table_name='funcloc')
    genduckdb.add_equi_table(sqlite_table_name='equi')
    genduckdb.add_classfloc_table(sqlite_table_name='classfloc')
    genduckdb.add_classequi_table(sqlite_table_name='classequi')
    genduckdb.add_valuafloc_table(sqlite_table_name='valuafloc')
    genduckdb.add_valuaequi_table(sqlite_table_name='valuaequi')
    genduckdb.add_classlist_tables(classlists_duckdb_path=classlists_db)
    genduckdb.gen_duckdb()

# Note - file downloads best used for checking so we need most of the information in equi downloads.
# Still need to know char_type to choose which field to look at for value in the valuaequi field.
