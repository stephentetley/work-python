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
import os
import duckdb
from typing import Callable
import polars as pl
from dataclasses import dataclass
from sptlibs.utils.asset_data_config import AssetDataConfig
from sptlibs.utils.xlsx_source import XlsxSource
import data_import.s4_ztables.duckdb_import as duckdb_import
import sptlibs.data_import.import_utils as import_utils


@dataclass
class ZTableInfo:
    name: str
    column_renames: dict[str, str]

def _get_ztable_files(*, source_dir: str) -> list[XlsxSource]:
    globlist = glob.glob('*.xlsx', root_dir=source_dir, recursive=False)
    def not_temp(file_name): 
        return not '~$' in file_name
    def expand(file_name): 
        return XlsxSource(os.path.normpath(os.path.join(source_dir, file_name)), 'Sheet1')
    return [expand(e) for e in globlist if not_temp(e)]



def get_table_props(df: pl.DataFrame) -> ZTableInfo | None:
    """Original column names are normalized from the Excel ztable dumps, text dumps have different names."""
    match df.columns: 
        case ['manufacturer_of_asset', 'model_number']: 
            return ZTableInfo('s4_ztables.manuf_model', 
                              {'manufacturer_of_asset': 'manufacturer', 
                               'model_number': 'model'})
        case ['object_type', 'manufacturer_of_asset', 'remarks']:
            return ZTableInfo('s4_ztables.objtype_manuf', 
                              {'object_type': 'objtype', 
                               'manufacturer_of_asset': 'manufacturer', 
                               'remarks': 'comments'})
        case ['object_type', 'object_type_1', 'equipment_category', 'remarks']:
            return ZTableInfo('s4_ztables.eqobjlbl', 
                              {'object_type': 'obj_parent',
                               'object_type_1': 'obj_child', 
                               'equipment_category': 'category',
                               'remarks': 'comments'})
        case ['object_type', 'standard_floc_description']:
            return ZTableInfo('s4_ztables.flocdes', 
                              {'object_type': 'objtype',
                               'standard_floc_description': 'description'})
        case ['structure_indicator', 'object_type', 'object_type_1', 'remarks']:
            return ZTableInfo('s4_ztables.floobjlbjl', 
                              {'object_type': 'obj_parent', 
                               'object_type_1': 'obj_child', 
                               'remarks': 'comments'})
        case _:
            return None


def main(): 
    parser = ArgumentParser(description='Generate ztable info DuckDB tables')
    parser.add_argument("--source_dir", dest='source_dir', required=True, help="Source directory containing AI2 exports")
    parser.add_argument("--output_db", dest='output_db', required=True, help="DuckDB file to add table to")
    args = parser.parse_args()
    source_directory    = args.source_dir


    output_db = args.output_db
    
    if source_directory and os.path.exists(source_directory):
        con = duckdb.connect(database=output_db, read_only=False)
        duckdb_import.create_duckdb_ztables(source_directory=source_directory, con=con)
        con.close()
        print(f"Done - created: {output_db}")

main()


