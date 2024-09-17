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
from argparse import ArgumentParser
import sptlibs.data_import.excel_table.duckdb_import as duckdb_import

def main():
    parser = ArgumentParser(description='Import an Excel table into a DuckDb database')
    parser.add_argument("--xls_source", dest='xls_source', required=True, help="Excel file to import")
    parser.add_argument("--sheet_name", dest='sheet_name', help="Name of sheet to read")
    parser.add_argument("--output_db", dest='output_db', required=True, help="DuckDB file to add table to")
    parser.add_argument("--table_name", dest='table_name', help="Optionally qualified table_name to write to")
    args = parser.parse_args()

    duckdb_import.import_excel_sheet(xls_path=args.xls_source, 
                                     sheet_name=args.sheet_name, 
                                     output_db=args.output_db,
                                     table_name=args.table_name)
    
main()
