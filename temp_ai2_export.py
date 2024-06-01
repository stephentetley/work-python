# temp_ai2_export.py

import polars as pl
import duckdb
from sptlibs.utils.asset_data_config import AssetDataConfig
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_import.s4_classlists.duckdb_import as classlists_duckdb_import
import sptlibs.data_import.ai2_export.duckdb_import as ai2_exports_import


config = AssetDataConfig()
config.set_focus('file_download_summary')

source1 = XlsxSource('G:/work/2024/ai2_to_s4/mal12/mal12-ai2-export-equimaster.xlsx', 'Sheet1')
source2 = XlsxSource('G:/work/2024/ai2_to_s4/mal12/mal12-ai2-export-asset-condition.xlsx', 'Sheet1')
source3 = XlsxSource('G:/work/2024/ai2_to_s4/mal12/mal12-ai2-export-emtrin.xlsx', 'Sheet1')
source4 = XlsxSource('G:/work/2024/ai2_to_s4/mal12/mal12-ai2-export-lstnco.xlsx', 'Sheet1')
source5 = XlsxSource('G:/work/2024/ai2_to_s4/mal12/mal12-ai2-export-pumpsc.xlsx', 'Sheet1')
source6 = XlsxSource('G:/work/2024/ai2_to_s4/mal12/mal12-ai2-export-stardo.xlsx', 'Sheet1')
source7 = XlsxSource('G:/work/2024/ai2_to_s4/mal12/mal12-ai2-export-trut.xlsx', 'Sheet1')
output_path = 'G:/work/2024/ai2_to_s4/mal12/mal12-aib.duckdb'


classlists_db = config.get_expanded_path('classlists_db_src')

conn = duckdb.connect(database=output_path)
classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)

ai2_exports_import.init(con=conn)
ai2_exports_import.import_ai2_exports([source1, source2, source3, source4, source5, source6, source7], con=conn)

conn.close()
print("done")
