# run_file_downloads.py

import os
from sptlibs.file_download.gen_duckdb import GenDuckdb

source_directory = 'g:/work/2024/file_download/valua_downloads'
glob_pattern = '*valua*.txt'
output_path = 'g:/work/2024/file_download/valua_downloads/valua.duckdb'
classlists_db = 'g:/work/2024/classlists/classlists.duckdb'



genduckdb = GenDuckdb(classlists_duckdb_path=classlists_db, duckdb_output_path=output_path)
genduckdb.add_downloads_source_directory(source_dir=source_directory, glob_pattern=glob_pattern)
genduckdb.gen_duckdb()

# Note - file downloads best used for checking so we need most of the information in equi downloads.
# Still need to know char_type to choose which field to look at for value in the valuaequi field.
