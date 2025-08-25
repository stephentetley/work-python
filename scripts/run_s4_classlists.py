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
import duckdb
import sptlibs.data_access.s4_classlists.s4_classlists_import as s4_classlists_import



def main(): 
    parser = ArgumentParser(description='Generate classlist info DuckDB tables')
    parser.add_argument("--equi_source_xlsx", dest='equi_source_xlsx', required=True, help="Equi classlist Excel export")
    parser.add_argument("--floc_source_xlsx", dest='floc_source_xlsx', required=True, help="Floc classlist Excel export")
    parser.add_argument("--output_db", dest='output_db', required=True, help="DuckDB file to add table to")
    args = parser.parse_args()
    equi_source = args.equi_source_xlsx
    floc_source = args.floc_source_xlsx
    output_db = args.output_db
    
    if equi_source and os.path.exists(equi_source) and floc_source and os.path.exists(floc_source):
        if os.path.exists(output_db):
            os.remove(output_db)
        
        con = duckdb.connect(database=output_db, read_only=False)
        s4_classlists_import.duckdb_import(equi_source_xlsx=equi_source, 
                                           floc_source_xlsx=floc_source, 
                                           con=con)
        con.close()
        print(f"Done - created: {output_db}")

main()



