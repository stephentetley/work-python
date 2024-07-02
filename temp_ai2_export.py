# temp_ai2_export.py

import glob
import duckdb
import os
from sptlibs.utils.asset_data_config import AssetDataConfig
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_import.s4_classlists.duckdb_import as classlists_duckdb_import
import sptlibs.data_import.ai2_export.duckdb_import as ai2_exports_import
import sptlibs.class_rep.ai2_class_rep.duckdb_init as ai2_class_duckdb_init


def get_downloads(*, source_dir: str, glob_pattern: str) -> list[XlsxSource]:
    globlist = glob.glob(glob_pattern, root_dir=source_dir, recursive=False)
    def expand(file_name): 
        return XlsxSource(os.path.normpath(os.path.join(source_dir, file_name)), 'Sheet1')
    return [expand(e) for e in globlist]


config = AssetDataConfig()
config.set_focus('file_download_summary')


glob_pattern        = '*.xlsx'

sources = get_downloads(source_dir = 'G:/work/2024/ai2_to_s4/wo100-telemetry', glob_pattern="sample*.xlsx")

print(sources)

output_path = 'G:/work/2024/ai2_to_s4/wo100-telemetry/wo100-telemetry-aib.duckdb'


classlists_db = config.get_expanded_path('classlists_db_src')

conn = duckdb.connect(database=output_path)
classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)

ai2_exports_import.init(con=conn)
ai2_exports_import.import_ai2_exports(sources, con=conn)

print("translating...")
ai2_class_duckdb_init.init(con=conn)

conn.close()
print("done")
