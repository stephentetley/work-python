# run_file_downloads.py

import os
from sptlibs.file_download.gen_duckdb import GenDuckdb

source_directories = ['g:/work/2023/file_download/test01']
glob_pattern = '*download.txt'
output_directory = 'g:/work/2023/file_download/test01'
classlists_db = 'g:/work/2023/classlist/classlists.duckdb'



if os.path.exists(output_directory): 
    print(f'Found: {output_directory}')

    genduckdb = GenDuckdb()
    genduckdb.set_output_directory(output_directory=output_directory)
    for dir in source_directories: 
        genduckdb.add_file_downloads_in_directory(path=dir, glob_pattern=glob_pattern)
    genduckdb.db_name = 'test01.duckdb'
    genduckdb.add_classlist_tables(classlists_duckdb_path=classlists_db)
    genduckdb.gen_duckdb()

# Note - file downloads best used for checking so we need most of the information in equi downloads.
# Still need to know char_type to choose which field to look at for value in the valuaequi field.
