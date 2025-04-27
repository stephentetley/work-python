# temp_concat_aide_changes.py
import os
import duckdb
import sptlibs.data_access.aide_changes.aide_changes as aide_changes

# DuckDB's `read_xlsx` not capable of using globs at the moment

xlsx_source_directory_glob = 'G:/work/2025/storm_overflow_reconcile/aide_concat/inbox-to-concat/*.xlsx'
duckdb_output_path = 'e:/coding/work/work-sql/aide_concat/concat1.duckdb'
xlsx_output_path = 'G:/work/2025/storm_overflow_reconcile/aide_concat/concat_output.xlsx' 

if os.path.exists(duckdb_output_path):
    os.remove(duckdb_output_path)

con = duckdb.connect(database=duckdb_output_path, read_only=False)
aide_changes.duckdb_init(xlsx_directory_glob=xlsx_source_directory_glob, con=con)
aide_changes.export_xlsx(xlsx_path=xlsx_output_path, con=con)
con.close()

