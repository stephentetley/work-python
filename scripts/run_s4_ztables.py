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

from argparse import ArgumentParser
import os
import duckdb
import sptlibs.data_access.s4_ztables.s4_ztables_import as s4_ztables_import
import sptlibs.data_access.import_utils as import_utils


def main(): 
    parser = ArgumentParser(description='Generate ztable info DuckDB tables')
    parser.add_argument("--source_dir", dest='source_dir', required=True, help="Source directory containing Ztable exports")
    parser.add_argument("--output_db", dest='output_db', required=True, help="DuckDB file to add table to")
    args = parser.parse_args()
    source_directory = args.source_dir
    output_db = args.output_db
    
    if source_directory and os.path.exists(source_directory):
        sources = import_utils.get_excel_sources_from_folder(source_folder = source_directory)
        con = duckdb.connect(database=output_db, read_only=False)
        s4_ztables_import.duckdb_import(sources=sources, con=con)
        con.close()
        print(f"Done - created: {output_db}")

main()


