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
import duckdb
from sptlibs.asset_data_config import AssetDataConfig
import sptlibs.data_import.s4_classlists.duckdb_import as duckdb_import



def main(): 
    parser = ArgumentParser(description='Generate equi and floc classlist DuckDB tables')
    _args = parser.parse_args()
    config = AssetDataConfig()
    config.set_focus('s4_classlists')
    equi_src = config.get_expanded_path('equi_classlist_src')
    floc_src = config.get_expanded_path('floc_classlist_src')
    output_path = config.get_expanded_path('s4_classlists_outfile')
    conn = duckdb.connect(database=output_path)
    duckdb_import.init(con=conn)
    duckdb_import.import_floc_classes(floc_src, con=conn)
    duckdb_import.import_equi_classes(equi_src, con=conn)
    conn.close()
    print(f"Done - created: {output_path}")

main()