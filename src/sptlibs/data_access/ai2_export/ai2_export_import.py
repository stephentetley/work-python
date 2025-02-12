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
from jinja2 import Template
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.utils.sql_script_runner2 import SqlScriptRunner2
import sptlibs.data_access.import_utils as import_utils

def duckdb_init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner2(__file__, con=con)
    runner.exec_sql_file(rel_file_path='ai2_export_create_tables.sql', con=con)

def duckdb_import(sources: list[XlsxSource], *, con: duckdb.DuckDBPyConnection) -> None:
    for src in sources:
        import_ai2_export(src, con=con)


def import_ai2_export(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    _import_master_data(xlsx, con=con)
    _import_eav_data(xlsx, con=con)

def _import_master_data(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT OR REPLACE INTO ai2_export.equi_master_data BY NAME
        SELECT 
            df.reference AS ai2_reference,
            df.common_name AS common_name,
            TRY_CAST(df.installed_from AS DATE) AS installed_from,
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


def _import_eav_data(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    df = import_utils.read_xlsx_source(xlsx, normalize_column_names=True)
    headers = df.columns
    headers.remove('reference')    
    headers = map(lambda x: f'{x}::VARCHAR', headers)
    header_str =', '.join(headers)
    insert_stmt = Template(_pivot_insert).render(headers=header_str)
    con.execute(insert_stmt)

_pivot_insert = """
    INSERT INTO ai2_export.equi_eav_data BY NAME
    SELECT 
        pvt.reference AS ai2_reference, 
        pvt.attr_name AS attribute_name, 
        trim(pvt.attr_value) AS attribute_value, 
    FROM (
        UNPIVOT df ON {{headers}} INTO NAME attr_name VALUE attr_value
    ) pvt
    WHERE
        pvt.attr_name NOT IN ('assetid', 'common_name', 'installed_from', 'manufacturer', 'model', 'hierarchy_key', 'assetstatus', 'asset_in_aide')
    ON CONFLICT DO UPDATE SET attribute_value = EXCLUDED.attribute_value;
"""