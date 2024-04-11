import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.data_import.ztables.duckdb_import as duckdb_import

output_path = 'g:/work/2024/ztables/ztables_new.duckdb'

conn = duckdb.connect(database=output_path)
duckdb_import.init(con=conn)
duckdb_import.import_manuf_model(XlsxSource('g:/work/2024/ztables/ztequi0075_manuf-manuf-model.xlsx', 'Sheet1'), con=conn)
duckdb_import.import_eqobjl(XlsxSource('g:/work/2023/ztables/zte_0343_eqobjl.XLSX', 'Sheet1'), con=conn)
duckdb_import.import_flocdes(XlsxSource('g:/work/2023/ztables/zte_0343_flocdes.XLSX', 'Sheet1'), con=conn)
duckdb_import.import_floobjl(XlsxSource('g:/work/2023/ztables/zte_0343_floobjl.XLSX', 'Sheet1'), con=conn)
duckdb_import.import_objtype_manuf(XlsxSource('g:/work/2023/ztables/ztequi0075_obj-objtype-manuf.XLSX', 'Sheet1'), con=conn)
conn.close()



