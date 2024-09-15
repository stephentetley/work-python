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
from pathlib import Path
import duckdb
from sptlibs.utils.xlsx_source import XlsxSource
from sptlibs.data_import.excel_table._excel_import_config import _ExcelImportConfig
import sptlibs.data_import.import_utils as import_utils





def import_excel_sheet(*, xls_path: str, toml_path: str, output_db: str | None) -> None:
    config = _ExcelImportConfig(toml_path=toml_path)
    
    if not output_db:
        output_dir = os.path.dirname(xls_path)
        file_name = Path(xls_path).stem + '.duckdb'
        output_db = os.path.join(output_dir, file_name)

    print(f"output to: {output_db}")

    sheet_name = config.get_excel_tab_name(alt='Sheet1')
    xls_source = XlsxSource(xls_path, sheet_name)

    if output_db:
        con = duckdb.connect(database=output_db, read_only=False)

        schema_name = config.get_schema_name()
        if schema_name:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

        qualified_table_name = config.get_qualified_tablename()
        if qualified_table_name:
            import_utils.duckdb_import_sheet(xls_source, qualified_table_name=qualified_table_name, con=con, df_trafo=None)

        con.close()
        print(f'wrote {output_db}')
    else:
        print(f'failed, check {toml_path}')
