import pandas as pd
from sptlibs.xlsx_source import XlsxSource
from sptlibs.ztables.gen_sqlite import GenSqlite

output_directory = 'g:/work/2023/ztables'

    
gensqlite = GenSqlite(output_directory=output_directory)
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/ztables/zte_0343_eqobjl.XLSX', 'Sheet1'), table_name='eqobjl')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/ztables/zte_0343_flocdes.XLSX', 'Sheet1'), table_name='flocdes')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/ztables/zte_0343_floobjl.XLSX', 'Sheet1'), table_name='floobjl')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/ztables/ztequi0075_manuf-manuf-model.xlsx', 'Sheet1'), table_name='manuf_model')
gensqlite.add_xlsx_source(XlsxSource('g:/work/2023/ztables/ztequi0075_obj-objtype-manuf.xlsx', 'Sheet1'), table_name='objtype_manuf')
sqlite_path = gensqlite.gen_sqlite()

