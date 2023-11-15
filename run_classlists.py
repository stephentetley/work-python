# run_classlists.py

from sptlibs.classlist.gen_duckdb import GenDuckdb

equi_src = 'g:/work/2023/classlist/002-equi-classlist.txt'
floc_src = 'g:/work/2023/classlist/003-floc-classlist.txt'
output_directory = 'g:/work/2023/classlist'

genduckdb = GenDuckdb(floc_classlist_path=floc_src, equi_classlist_path=equi_src)
genduckdb.set_output_directory(output_directory=output_directory)
genduckdb.set_db_name(db_name='classlist2.duckdb')
out_path = genduckdb.gen_duckdb()
print(out_path)


