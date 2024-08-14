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

import os
import duckdb
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.data_import.excel_table._excel_import_config import _ExcelImportConfig
import sptlibs.data_import.import_utils as import_utils





def import_excel_sheet(*, xls_path: str, toml_path: str, output_dir: str | None) -> None:
    config = _ExcelImportConfig(toml_path=toml_path)
    
    if not output_dir:
        output_dir = os.path.dirname(xls_path)

    print(f"output to: {output_dir}")

    sheet_name = config.get_excel_tab_name(alt='Sheet1')
    xls_source = XlsxSource(xls_path, sheet_name)

    duckdb_output_path  = config.get_db_output_path(output_dir=output_dir)
    print(duckdb_output_path)
    if duckdb_output_path:
        con = duckdb.connect(database=duckdb_output_path, read_only=False)

        schema_name = config.get_schema_name()
        if schema_name:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

        qualified_table_name = config.get_qualified_tablename()
        if qualified_table_name:
            import_utils.duckdb_import_sheet(xls_source, qualified_table_name=qualified_table_name, con=con, df_trafo=None)

        con.close()
        print(f'wrote {duckdb_output_path}')
    else:
        print(f'failed, check {toml_path}')
