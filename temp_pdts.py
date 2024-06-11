import duckdb
from sptlibs.data_import.pdt.pdt_file import PdtFiles
import sptlibs.data_import.pdt.duckdb_import as duckdb_import

duckdb_path = 'G:/work/2024/pdts/har65-pdt-summary.duckdb'

con = duckdb.connect(database=duckdb_path, read_only=False)

duckdb_import.init(con=con)

pdts = PdtFiles.from_files(src_dir='G:/work/2024/pdts/har65')

pdts.store(con=con)

duckdb_import.build_class_rep(con=con)

con.close()
print(f"Wrote: `{duckdb_path}`")
