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
import sptlibs.data_import.rts_outstations.duckdb_import as duckdb_import

# TODO specify input file at command line...
def main(): 
    parser = ArgumentParser(description='Generate rts outstation info table from an rts outstations report')
    parser.add_argument("--report_outfile", dest='report_outfile', help="cvs output path, use to optionally generate a csv report")
    args = parser.parse_args()
    config = AssetDataConfig()
    config.set_focus('rts_outstations')

    csv_input_path = config.get_expanded_path('rts_outstations_src')
    duckdb_output_path = config.get_expanded_path('rts_outstations_outfile')

    conn = duckdb.connect(database=duckdb_output_path)
    duckdb_import.import_outstations_report(csv_input_path, con=conn)

    if args.report_outfile:
        csv_output_path = os.path.normpath(args.report_outfile)
        export_stmt = f"COPY rts_raw_data.outstations_report TO '{csv_output_path}' (HEADER, DELIMITER ',');"
        conn.execute(export_stmt)
        print(f"{csv_output_path} created")
    
    conn.close()
    print(f"{duckdb_output_path} created")

main()
