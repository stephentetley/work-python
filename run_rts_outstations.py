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
import sptlibs.data_import.rts_outstations.duckdb_import as duckdb_import

# TODO specify input file at command line...
def main(): 
    parser = ArgumentParser(description='Generate rts outstation info table from an rts outstations report')
    parser.add_argument("--source_file", dest='source_file', required=True, help="Source file - tab separated csv")
    parser.add_argument("--dest_dir", dest='dest_dir', required=True, help="Directory for DuckDb and Csv output")
    args = parser.parse_args()
    source_file = args.source_file
    dest_directory = args.dest_dir

    if source_file and os.path.exists(source_file) and os.path.exists(dest_directory):

        duckdb_output_path = os.path.normpath(os.path.join(dest_directory, "outstations_db.duckdb"))

        conn = duckdb.connect(database=duckdb_output_path)
        duckdb_import.import_outstations_report(source_file, con=conn)

        csv_output_path = os.path.normpath(os.path.join(dest_directory, "outstations_tidy.csv"))
        export_stmt = f"COPY rts_raw_data.outstations_report TO '{csv_output_path}' (HEADER, DELIMITER ',');"
        conn.execute(export_stmt)
        print(f"{csv_output_path} created")
        
        conn.close()
        print(f"{duckdb_output_path} created")

main()
