# run_level1_floc_mapping.py

import duckdb
import pandas as pd
from sptlibs.xlsx_source import XlsxSource
import sptapps.floc_mapping.load_raw_data as load_raw_data
import sptapps.floc_mapping.duckdb_setup as duckdb_setup
from sptlibs.data_frame_xlsx_table import DataFrameXlsxTable

duckdb_output_path  = 'g:/work/2024/floc-sai-mapping/2024_01_22/jan22-floc-sai2.duckdb'
xlsx_output_path    = 'g:/work/2024/floc-sai-mapping/2024_01_22/jan22_level1_children_report.xlsx'

# {"dt": pl.Int64}

raw_tables = [
    ('sai_raw_data.ai2_data',        XlsxSource('g:/work/2024/floc-sai-mapping/2024_01_12/AI2AssetHierarchy_20240112.xlsx', 'Sheet1'), None),
    ('sai_raw_data.s4_level_1_2',    XlsxSource('g:/work/2024/floc-sai-mapping/2024_01_12/l1-l2-export-2024.01.12.xlsx', 'Sheet1'), None),
    ('sai_raw_data.site_mapping',    XlsxSource('g:/work/2024/floc-sai-mapping/2024_01_12/site_mapping1.xlsx', 'inst to SAP migration'), None)
]


con = duckdb.connect(database=duckdb_output_path, read_only=False)
load_raw_data.load_raw_data(tables=raw_tables, con=con)
duckdb_setup.setup_views(con=con)

with pd.ExcelWriter(xlsx_output_path) as xlwriter: 
    query = 'SELECT * FROM s4_sai_mapping.vw_level1_children_report ORDER BY funcloc;'
    con.execute(query=query)
    df1 = con.df()
    table = DataFrameXlsxTable(df=df1)
    table.to_excel(writer=xlwriter, sheet_name='Sheet1')
    con.close()
    print(f'Wrote: {xlsx_output_path}')

con.close()
