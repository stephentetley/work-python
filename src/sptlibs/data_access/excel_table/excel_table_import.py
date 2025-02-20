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
import sptlibs.data_access.import_utils as import_utils


def duckdb_import_table(*, xls_path: str, sheet_name: str | None, output_db: str | None, table_name: str | None) -> None:
    if not output_db:
        output_dir = os.path.dirname(xls_path)
        file_name = Path(xls_path).stem + '.duckdb'
        output_db = os.path.join(output_dir, file_name)

    print(f"output to: {output_db}")
    if output_db:
        con = duckdb.connect(database=output_db, read_only=False)
        duckdb_import(con=con, xls_path=xls_path, sheet_name=sheet_name, table_name=table_name)
        con.close()
        print(f'wrote {output_db}')
    else:
        print(f'failed, check {output_db}')

def duckdb_import(xls_path: str, *, 
                  con: duckdb.DuckDBPyConnection,
                  table_name: str | None =None,
                  sheet_name : str | None =None,) -> None:
    
    if not sheet_name:
        sheet_name = 'Sheet1'

    xls_source = XlsxSource(xls_path, sheet_name)
    
    if table_name:
        (schema_name, _, _) = table_name.partition('.')
    else:
        schema_name = None
        table_name = import_utils.normalize_name(sheet_name)

    print(f'schema_name: {schema_name}, table_name: {table_name}')
    if schema_name:
        con.execute(query=f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    if table_name:
        import_utils.duckdb_import_sheet(xls_source, qualified_table_name=table_name, con=con, df_trafo=None)
        print(f'wrote {table_name}')
    else:
        print(f'fail table name not recognized')
