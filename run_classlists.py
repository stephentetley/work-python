# run_classlists.py

import duckdb
import sptlibs.data_import.classlists.duckdb_import as duckdb_import

equi_src = 'g:/work/2024/classlists/002-equi-classlist-feb24.txt'
floc_src = 'g:/work/2024/classlists/003-floc-classlist.txt'
output_path = 'g:/work/2024/classlists/classlists2.duckdb'


conn = duckdb.connect(database=output_path)
duckdb_import.init(con=conn)
duckdb_import.import_floc_classes(floc_src, con=conn)
duckdb_import.import_equi_classes(equi_src, con=conn)
conn.close()
print("Done")

