# run_floc_mapping_setup.py

import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptapps.floc_mapping.load_raw_data as load_raw_data

duckdb_output_path = 'g:/work/2024/floc-sai-mapping/2024_01_12/jan12-floc-sai.duckdb'


raw_tables = {
    'sai_raw_data.ai2_data':        XlsxSource('g:/work/2024/floc-sai-mapping/2024_01_12/AI2AssetHierarchy_20240112.xlsx', 'Sheet1'),
    #'sai_raw_data.s4_level_1_2':    XlsxSource('g:/work/2024/floc-sai-mapping/2024_01_12/l1-l2-export-2024.01.12.xlsx', 'Sheet1'),
    #'sai_raw_data.site_mapping':    XlsxSource('g:/work/2024/floc-sai-mapping/2024_01_12/site_mapping1.xlsx', 'inst to SAP migration')
}


con = duckdb.connect(database=duckdb_output_path, read_only=False)
load_raw_data.load_raw_data(tables=raw_tables, con=con)
con.close()

## see: https://docs.pola.rs/py-polars/html/reference/api/polars.read_excel.html