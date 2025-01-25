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


from dataclasses import dataclass
import duckdb
import polars as pl
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_access.import_utils as import_utils



def duckdb_import(*, sources: list[XlsxSource], con: duckdb.DuckDBPyConnection) -> None:
    con.execute('CREATE SCHEMA IF NOT EXISTS s4_ztables;')
    for source in sources:
        print(source)
        df = import_utils.read_xlsx_source(source, normalize_column_names=True)
        info = _get_table_props(df)
        if info:
            df1 = df.rename(info.column_renames)
            import_utils.duckdb_store_polars_dataframe(df1, 
                                                        table_name=info.table_name,
                                                        con=con)


@dataclass
class _ZTableInfo:
    table_name: str
    column_renames: dict[str, str]




def _get_table_props(df: pl.DataFrame) -> _ZTableInfo | None:
    """Original column names are normalized from the Excel ztable dumps, text dumps have different names."""
    match df.columns: 
        case ['manufacturer_of_asset', 'model_number']: 
            return _ZTableInfo('s4_ztables.manuf_model', 
                               {'manufacturer_of_asset': 'manufacturer', 
                                'model_number': 'model'})
        case ['object_type', 'manufacturer_of_asset', 'remarks']:
            return _ZTableInfo('s4_ztables.objtype_manuf', 
                               {'object_type': 'objtype', 
                                'manufacturer_of_asset': 'manufacturer', 
                                'remarks': 'comments'})
        case ['object_type', 'object_type_1', 'equipment_category', 'remarks']:
            return _ZTableInfo('s4_ztables.eqobjlbl', 
                                {'object_type': 'obj_parent',
                                'object_type_1': 'obj_child', 
                                'equipment_category': 'category',
                                'remarks': 'comments'})
        case ['object_type', 'standard_floc_description']:
            return _ZTableInfo('s4_ztables.flocdes', 
                                {'object_type': 'objtype',
                                 'standard_floc_description': 'description'})
        case ['structure_indicator', 'object_type', 'object_type_1', 'remarks']:
            return _ZTableInfo('s4_ztables.floobjlbjl', 
                               {'object_type': 'obj_parent', 
                                'object_type_1': 'obj_child', 
                                'remarks': 'comments'})
        case _:
            return None


def copy_ztable_tables(*, source_db_path: str, dest_con: duckdb.DuckDBPyConnection) -> None:
    """`dest_con` is the desination database."""
    # copy tables using duckdb builtins
    import_utils.duckdb_import_tables_from_duckdb(source_db_path=source_db_path, 
                                                  con=dest_con,
                                                  schema_name='s4_ztables',
                                                  source_tables=['s4_ztables.manuf_model',
                                                                 's4_ztables.objtype_manuf', 
                                                                 's4_ztables.eqobjlbl',
                                                                 's4_ztables.flocdes',
                                                                 's4_ztables.floobjlbjl'])
