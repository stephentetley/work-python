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
from sptlibs.utils.asset_data_config import AssetDataConfig
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_import.s4_ztables.duckdb_import as duckdb_import


def main(): 
    parser = ArgumentParser(description='Generate ztable info DuckDB tables')
    _args = parser.parse_args()
    config = AssetDataConfig()
    config.set_focus('s4_ztables')
    eqobjl_src = config.get_expanded_path('eqobjl_src')
    flocdes_src = config.get_expanded_path('flocdes_src')
    floobjl_src = config.get_expanded_path('floobjl_src')
    manuf_model_src = config.get_expanded_path('manuf_model_src')
    objtype_src = config.get_expanded_path('objtype_src')
    output_path = config.get_expanded_path('s4_ztables_outfile')

    conn = duckdb.connect(database=output_path)
    duckdb_import.init(con=conn)
    duckdb_import.import_eqobjl(XlsxSource(eqobjl_src, 'Sheet1'), con=conn)
    duckdb_import.import_flocdes(XlsxSource(flocdes_src, 'Sheet1'), con=conn)
    duckdb_import.import_floobjl(XlsxSource(floobjl_src, 'Sheet1'), con=conn)
    duckdb_import.import_manuf_model(XlsxSource(manuf_model_src, 'Sheet1'), con=conn)
    duckdb_import.import_objtype_manuf(XlsxSource(objtype_src, 'Sheet1'), con=conn)
    conn.close()
    print(f"Done - created: {output_path}")

main()


