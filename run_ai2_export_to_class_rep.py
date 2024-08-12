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
import sptlibs.data_import.s4_classlists.duckdb_import as classlists_duckdb_import
import sptlibs.data_import.ai2_export.duckdb_import as ai2_exports_import
import sptlibs.class_rep.ai2_class_rep.duckdb_init as ai2_class_duckdb_init



def _get_downloads(*, source_dir: str, glob_pattern: str) -> list[XlsxSource]:
    globlist = glob.glob(glob_pattern, root_dir=source_dir, recursive=False)
    def expand(file_name): 
        return XlsxSource(os.path.normpath(os.path.join(source_dir, file_name)), 'Sheet1')
    return [expand(e) for e in globlist]

def main():
    parser = ArgumentParser(description='Generate a class_rep database for AI2 exports')
    parser.add_argument("--source_dir", dest='source_dir', required=True, help="Source directory containing AI2 exports")
    parser.add_argument("--dest_dir", dest='dest_dir', help="Destination directory for the report")
    parser.add_argument("--glob_pattern", dest='glob_pattern', help="Glob pattern to identify *.xlsx files")
    parser.add_argument("--db_prefix", dest='db_prefix', help="Prefix to be added to the database name")
    args = parser.parse_args()
    source_directory    = args.source_dir

    config = AssetDataConfig()
    config.set_focus('file_download_summary')

    glob_pattern    = args.glob_pattern
    if not glob_pattern:
        glob_pattern = '*.xlsx'

    if args.dest_dir: 
        dest_directory = args.dest_dir
    else:
        dest_directory = source_directory
    
    if args.db_prefix:
        output_db_name = args.db_prefix + 'ai2_class_rep_db.duckdb'
    else:
        output_db_name = 'ai2_class_rep_db.duckdb'

    if source_directory and os.path.exists(source_directory) and os.path.exists(dest_directory):
        sources = _get_downloads(source_dir = source_directory, glob_pattern=glob_pattern)
        for source in sources:
            print(source)

        output_path = os.path.join(dest_directory, output_db_name) 

        classlists_db = config.get_expanded_path('classlists_db_src')

        conn = duckdb.connect(database=output_path)
        classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)

        ai2_exports_import.init(con=conn)
        ai2_exports_import.import_ai2_exports(sources, con=conn)

        print("translating...")
        ai2_class_duckdb_init.init(con=conn)

        conn.close()
        print(f"Created - {output_path}")

main()
