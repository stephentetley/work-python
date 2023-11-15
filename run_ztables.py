import pandas as pd
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ztables.gen_duckdb import GenDuckdb

output_directory = 'g:/work/2023/ztables'

    
genduckdb = GenDuckdb()
genduckdb.set_output_directory(output_directory=output_directory)
genduckdb.add_xlsx_source(XlsxSource('g:/work/2023/ztables/zte_0343_eqobjl.XLSX', 'Sheet1'), table_name='eqobjl')
genduckdb.add_xlsx_source(XlsxSource('g:/work/2023/ztables/zte_0343_flocdes.XLSX', 'Sheet1'), table_name='flocdes')
genduckdb.add_xlsx_source(XlsxSource('g:/work/2023/ztables/zte_0343_floobjl.XLSX', 'Sheet1'), table_name='floobjl')
genduckdb.add_xlsx_source(XlsxSource('g:/work/2023/ztables/ztequi0075_manuf-manuf-model.xlsx', 'Sheet1'), table_name='manuf_model')
genduckdb.add_xlsx_source(XlsxSource('g:/work/2023/ztables/ztequi0075_obj-objtype-manuf.xlsx', 'Sheet1'), table_name='objtype_manuf')
output = genduckdb.gen_duckdb()
print(output)

