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
from sptlibs.utils.asset_data_config import AssetDataConfig
import sptlibs.data_access.s4_classlists.s4_classlists_import as classlists_duckdb_import
import sptlibs.data_access.file_download.file_download_import as file_download_import
import sptlibs.class_rep.s4_class_rep.duckdb_init as s4_class_rep_duckdb_setup
import sptapps.reports.s4_class_rep_report.gen_report as gen_report



def main():
    parser = ArgumentParser(description='Generate a summary report for equi/floc file downloads')
    parser.add_argument("--source_dir", dest='source_dir', required=True, help="Source directory containing file downloads")
    parser.add_argument("--glob_pattern", dest='glob_pattern', help="Glob to identify file downloads")
    parser.add_argument("--dest_dir", dest='dest_dir', help="Destination directory for the report")
    parser.add_argument("--report_prefix", dest='report_prefix', help="Prefix to be added to report name")
    parser.add_argument("--report_name", dest='report_name', help="Report name, use instead of `report_prefix` to specify full name")
    args = parser.parse_args()

    source_directory    = args.source_dir
    glob_pattern    = args.glob_pattern
    if not glob_pattern:
        glob_pattern = '*download.txt'

    config = AssetDataConfig()
    classlists_db = config.get_classlists_db()

    if args.dest_dir: 
        dest_directory = args.dest_dir
    else:
        dest_directory = source_directory

    report_name = 'fd-summary-report.xlsx'
    output_db_name = 'fd_summary_data.duckdb'
    if args.report_name: 
        report_name = args.report_name
    elif args.report_prefix:
        report_name = args.report_prefix + report_name
        output_db_name = args.report_prefix + output_db_name

    if source_directory and os.path.exists(source_directory) and os.path.exists(dest_directory):
        duckdb_output_path  = os.path.join(dest_directory, output_db_name)
        xlsx_output_path    = os.path.join(dest_directory, report_name)
        conn = duckdb.connect(database=duckdb_output_path)
        classlists_duckdb_import.copy_classlists_tables(classlists_source_db_path=classlists_db, setup_tables=True, dest_con=conn)

        file_download_import.duckdb_table_init(con=conn)
        file_download_import.duckdb_import_directory(source_dir=source_directory, glob_pattern=glob_pattern, con=conn)

        s4_class_rep_duckdb_setup.init_s4_class_rep_tables(con=conn)
        gen_report.gen_report(xls_output_path=xlsx_output_path, con=conn)
        conn.close()
        print(f"Created - {xlsx_output_path}")

main()
