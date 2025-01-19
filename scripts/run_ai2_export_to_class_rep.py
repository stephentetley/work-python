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
import glob
import duckdb
import os
from sptlibs.utils.asset_data_config import AssetDataConfig
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_access.s4_classlists.s4_classlists_import as classlists_duckdb_import
import sptlibs.data_access.ai2_export.ai2_export_import as ai2_exports_import
import sptlibs.classrep.ai2_classrep.ai2_classrep_setup as ai2_classrep_setup



def _get_downloads(*, source_dir: str, glob_pattern: str) -> list[XlsxSource]:
    globlist = glob.glob(glob_pattern, root_dir=source_dir, recursive=False)
    def not_temp(file_name): 
        return not '~$' in file_name
    def expand(file_name): 
        return XlsxSource(os.path.normpath(os.path.join(source_dir, file_name)), 'Sheet1')
    return [expand(e) for e in globlist if not_temp(e)]

def main():
    parser = ArgumentParser(description='Generate a class_rep database for AI2 exports')
    parser.add_argument("--source_dir", dest='source_dir', required=True, help="Source directory containing AI2 exports")
    parser.add_argument("--glob_pattern", dest='glob_pattern', help="Glob pattern to identify *.xlsx files")
    parser.add_argument("--output_db", dest='output_db', required=True, help="DuckDB file to import into")
    args = parser.parse_args()
    source_directory    = args.source_dir

    config = AssetDataConfig()

    glob_pattern    = args.glob_pattern
    if not glob_pattern:
        glob_pattern = '*.xlsx'
   
    output_db = args.output_db
    
    if source_directory and os.path.exists(source_directory):
        sources = _get_downloads(source_dir = source_directory, glob_pattern=glob_pattern)
        for source in sources:
            print(source)

        classlists_db = config.get_classlists_db()

        conn = duckdb.connect(database=output_db)
        classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)

        ai2_exports_import.duckdb_init(con=conn)
        ai2_exports_import.duckdb_import(sources, con=conn)

        print("translating...")
        ai2_classrep_setup.duckdb_init(con=conn)

        conn.close()
        print(f"Wrote - {output_db}")

main()
