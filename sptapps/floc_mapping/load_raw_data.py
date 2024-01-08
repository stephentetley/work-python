"""
Copyright 2024 Stephen Tetley

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import duckdb
import sptlibs.import_utils as import_utils
from sptlibs.xlsx_source import XlsxSource

def load_raw_data(*, tables: dict, con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS sai_raw_data;")
    for key, value in tables.items():
        import_utils.duckdb_import_sheet(value, table_name=key, con=con, df_trafo=None)

# import_utils.duckdb_import_sheet(XlsxSource('g:/work/2023/floc-sais/AI2AssetHierarchy_20231208.xlsx', 'Sheet1'), table_name='raw_data.ai2_data', con=con, df_trafo=None)

# import_utils.duckdb_import_sheet(XlsxSource('g:/work/2023/floc-sais/SAPLv1_2withAI2Ref.xlsx', 'Sheet1'), table_name='raw_data.s4_level_1_2', con=con, df_trafo=None)
# import_utils.duckdb_import_sheet(XlsxSource('g:/work/2023/floc-sais/FLOCLV3with AI2Ref.xlsx', 'Sheet1'), table_name='raw_data.s4_level_3', con=con, df_trafo=None)
# import_utils.duckdb_import_sheet(XlsxSource('g:/work/2023/floc-sais/FLOCLV4with AI2Ref.xlsx', 'Sheet1'), table_name='raw_data.s4_level_4', con=con, df_trafo=None)
# import_utils.duckdb_import_sheet(XlsxSource('g:/work/2023/floc-sais/FLOCLV5with AI2Ref.xlsx', 'Sheet1'), table_name='raw_data.s4_level_5', con=con, df_trafo=None)

# import_utils.duckdb_import_sheet(XlsxSource('g:/work/2023/floc-sais/site_mapping.xlsx', 'inst to SAP migration'), table_name='raw_data.site_mapping', con=con, df_trafo=None)
