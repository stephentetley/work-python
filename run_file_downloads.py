# run_file_downloads.py

import os
import glob
import sptlibs.file_download.file_download_parser as file_download_parser
from sptlibs.file_download.gen_sqlite import GenSqlite
from sptlibs.file_download.gen_duckdb import GenDuckdb

source_directories = ['g:/work/2023/file_download/equi-class-chars-sample',
                        'g:/work/2023/file_download/floc-class-chars-sample']
                      
output_directory = 'g:/work/2023/file_download'
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'

downloads = []
for src in source_directories: 
    globlist = glob.glob('*download.txt', root_dir=src, recursive=False)
    for answer in globlist: 
        downloads.append(os.path.join(src, answer))

for x in downloads: 
    print(x)

# downloads = ['g:/work/2023/file_download/equi-class-chars-sample/equi_download.txt', 
#                 'g:/work/2023/file_download/equi-class-chars-sample/classequi_download.txt', 
#                 'g:/work/2023/file_download/equi-class-chars-sample/valuaequi_download.txt',
#                 'g:/work/2023/file_download/floc-class-chars-sample/funcloc_download.txt',
#                 'g:/work/2023/file_download/floc-class-chars-sample/classfloc_download.txt',
#                 'g:/work/2023/file_download/floc-class-chars-sample/valuafloc_download.txt'
#              ]

if os.path.exists(output_directory): 
    print(f'Found: {output_directory}')

    # TODO - custom functions for entity_types?
    gensqlite = GenSqlite(output_directory=output_directory)
    gensqlite.db_name = 'file_download_equi_floc2.sqlite3'
    for download_path in downloads: 
        gensqlite.add_file_download(path=download_path)
    sqlite_path = gensqlite.gen_sqlite()

    genduckdb = GenDuckdb(sqlite_path=sqlite_path, output_directory=output_directory)
    genduckdb.db_name = 'file_download_equi_floc2.duckdb'
    genduckdb.add_classlist_tables(classlists_duckdb_path=classlists_db)
    genduckdb.gen_duckdb()

# Note - file downloads best used for checking so we need most of the information in equi downloads.
# Still need to know char_type to choose which field to look at for value in the valuaequi field.
