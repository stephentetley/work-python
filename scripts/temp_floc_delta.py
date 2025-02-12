# temp_floc_delta.py

import duckdb
import sptapps.floc_builder.generate_flocs as generate_flocs

duckdb_path = 'E:/coding/work/work-python/src/sptapps/floc_builder/runtime/downloads/floc_builder.duckdb'
ztable_source = 'E:/coding/work/work-python/src/sptapps/floc_builder/runtime/config/s4_ztables_latest.duckdb'
worklist_path = 'g:/work/2025/floc_builder/har55_source/har55_worklist.with_notes.xlsx'
ih06_path = 'g:/work/2025/floc_builder/har55_source/HAR55_ih06_20250110095604.xlsx'


con = duckdb.connect(database=duckdb_path, read_only=False)
generate_flocs.generate_flocs(worklist_path=worklist_path,
                              ih06_path=ih06_path,
                              ztable_source_db=ztable_source,
                              con=con)
con.close()

print(f"Done - added raw data to: {duckdb_path}")

