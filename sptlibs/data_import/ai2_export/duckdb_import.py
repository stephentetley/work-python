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
from sptlibs.xlsx_source import XlsxSource
import sptlibs.data_import.import_utils as import_utils
import sptlibs.data_import.ai2_export.duckdb_setup as duckdb_setup

def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    duckdb_setup.setup_tables(con=con)

def import_ai2_export(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    __import_master_data(xlsx, con=con)
    __import_eav_data(xlsx, con=con)

def __import_master_data(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT OR REPLACE INTO ai2_export.master_data BY NAME
        SELECT 
            df.reference AS ai2_reference,
            df.common_name AS common_name,
            strptime(df.installed_from, '%m/%d/%y %H:%M') AS installed_from,
            df.manufacturer AS manufacturer,
            df.model AS model,
            df.hierarchy_key AS hierarchy_key,
            df.assetstatus AS asset_status,
            IF(df.asset_in_aide = 'FALSE', false, true) AS asset_in_aide,
        FROM 
            df_dataframe_view df
        ;
        """
    import_utils.duckdb_import_sheet_into(xlsx, df_name='df_dataframe_view', insert_stmt=insert_stmt, df_trafo=None, con=con)


def __import_eav_data(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    df = import_utils.readXlsxSource(xlsx, normalize_column_names=True, con=con)
    headers = df.columns
    headers.remove('reference')    
    headers = map(lambda x: f'{x}::VARCHAR', headers)
    header_str =', '.join(headers)
    pivot_insert = f"""
        INSERT INTO ai2_export.eav_data BY NAME
        SELECT 
            pvt.reference AS ai2_reference, 
            pvt.attr_name AS attribute_name, 
            pvt.attr_value AS attribute_value, 
        FROM (
            UNPIVOT df ON {header_str} INTO NAME attr_name VALUE attr_value
        ) pvt
        WHERE
            pvt.attr_name NOT IN ('assetid', 'common_name', 'installed_from', 'manufacturer', 'model', 'hierarchy_key', 'assetstatus', 'asset_in_aide')
        ON CONFLICT DO UPDATE SET attribute_value = EXCLUDED.attribute_value;
    """
    con.execute(pivot_insert)