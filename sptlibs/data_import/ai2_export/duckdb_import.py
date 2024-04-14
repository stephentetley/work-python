"""
Copyright 2023 Stephen Tetley

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
from sptlibs.xlsx_source import XlsxSource
import sptlibs.data_import.import_utils as import_utils
import sptlibs.data_import.ai2_export.duckdb_setup as duckdb_setup

def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    duckdb_setup.setup_tables(con=con)


def import_ai2_export(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO ai2_export.master_data BY NAME
        SELECT 
            df.reference AS ai2_reference,
            df.common_name AS common_name,
            strptime(df.installed_from, '%m/%d/%y %H:%M') AS installed_from,
            df.manufacturer AS manufacturer,
            df.model AS model,
            df.hierarchy_key AS hierarchy_key,
            df.assetstatus AS asset_status,
            df.asset_in_aide AS asset_in_aide,
        FROM 
            df_dataframe_view df
        ;
        """
    import_utils.duckdb_import_sheet_into(xlsx, df_name='df_dataframe_view', insert_stmt=insert_stmt, df_trafo=None, con=con)