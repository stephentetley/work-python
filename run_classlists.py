# run_classlists.py

from sptlibs.classlist.gen_duckdb import GenDuckdb

equi_src = 'g:/work/2024/classlists/002-equi-classlist-feb24.txt'
floc_src = 'g:/work/2024/classlists/003-floc-classlist.txt'
output_db_path = 'g:/work/2024/classlists/classlists.duckdb'

gen_duckdb = GenDuckdb(floc_classlist_path=floc_src, equi_classlist_path=equi_src, duckdb_output_path=output_db_path)
out_path = gen_duckdb.gen_duckdb()
print(out_path)


