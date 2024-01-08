# run_floc_mapping_setup.py

import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptapps.floc_mapping.load_raw_data as load_raw_data

duckdb_path = 'g:/work/2024/floc-sai-mapping/floc-sai-mapping1.duckdb'


raw_tables = {
    'sai_raw_data.ai2_data':        XlsxSource('g:/work/2023/floc-sais/AI2AssetHierarchy_20231208.xlsx', 'Sheet1'),
    'sai_raw_data.s4_level_1_2':    XlsxSource('g:/work/2023/floc-sais/SAPLv1_2withAI2Ref.xlsx', 'Sheet1'),
    'sai_raw_data.s4_level_3':      XlsxSource('g:/work/2023/floc-sais/FLOCLV3with AI2Ref.xlsx', 'Sheet1'),
    'sai_raw_data.s4_level_4':      XlsxSource('g:/work/2023/floc-sais/FLOCLV4with AI2Ref.xlsx', 'Sheet1'),
    'sai_raw_data.s4_level_5':      XlsxSource('g:/work/2023/floc-sais/FLOCLV5with AI2Ref.xlsx', 'Sheet1'),
    'sai_raw_data.site_mapping':    XlsxSource('g:/work/2023/floc-sais/site_mapping.xlsx', 'inst to SAP migration')
}


con = duckdb.connect(database=duckdb_path, read_only=False)
load_raw_data.load_raw_data(tables=raw_tables, con=con)
con.close()