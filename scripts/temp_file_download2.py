# temp_concat_aide_changes.py
import os
import duckdb
import sptlibs.data_access.file_download2.file_download_import as file_download_import

# DuckDB's `read_xlsx` not capable of using globs at the moment

source_directory_glob = 'G:/work/2025/new_file_download/fra05/file_*.txt'
duckdb_output_path = 'e:/coding/work/work-sql/new_file_download/fra05_downloads.duckdb'

if os.path.exists(duckdb_output_path):
    os.remove(duckdb_output_path)

con = duckdb.connect(database=duckdb_output_path, read_only=False)
file_download_import.duckdb_init(directory_glob=source_directory_glob, con=con)
con.close()

