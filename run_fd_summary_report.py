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
from asset_data_config import AssetDataConfig
import sptlibs.data_import.file_download.duckdb_import as duckdb_import
import sptlibs.data_import.classlists.duckdb_import as classlists_duckdb_import
from sptlibs.reports.file_download_summary.gen_summary_report import GenSummaryReport



def main():
    parser = ArgumentParser()
    parser.add_argument("--source_dir", dest='source_dir', help="Source directory containing file downloads")
    parser.add_argument("--dest_dir", dest='dest_dir', help="Destination directory for the report")
    parser.add_argument("--report_prefix", dest='report_prefix', help="Prefix to be added to report name")
    parser.add_argument("--report_name", dest='report_name', help="Report name, use instead of `report_prefix` to specify full name")
    args = parser.parse_args()
    config = AssetDataConfig()
    config.set_focus('file_download_summary')

    classlists_db = config.get_expanded_path('classlists_db_src')
    print(classlists_db)

    glob_pattern        = config.get('glob_pattern', '*download.txt')
    source_directory    = args.source_dir

    if args.dest_dir: 
        dest_directory = args.dest_dir
    else:
        dest_directory = source_directory

    report_name = 'fd-summary-report.xlsx'
    if args.report_name: 
        report_name = args.report_name
    elif args.report_prefix:
        report_name = args.report_prefix + report_name

    if source_directory and os.path.exists(source_directory) and os.path.exists(dest_directory):
        duckdb_output_path  = os.path.join(dest_directory, 'fd_summary_data.duckdb')
        xlsx_output_path    = os.path.join(dest_directory, report_name)
        conn = duckdb.connect(database=duckdb_output_path)
        duckdb_import.init(con=conn)
        duckdb_import.store_download_files(source_dir=source_directory, glob_pattern=glob_pattern, con=conn)
        classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)
        conn.close()
        gen_summary = GenSummaryReport(db_path=duckdb_output_path, xlsx_output_name=xlsx_output_path)
        gen_summary.gen_summary_report()

main()
