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

import duckdb
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_import.import_utils as import_utils
from sptlibs.utils.sql_script_runner import SqlScriptRunner



def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    runner = SqlScriptRunner()
    runner.exec_sql_file(file_rel_path='s4_ztables/s4_ztables_create_tables.sql', con=con)



def import_manuf_model(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
        df = import_utils.read_xlsx_source(xlsx, normalize_column_names=True)
        import_utils.duckdb_write_dataframe_to_table(
             df, con=con, 
             qualified_table_name='s4_ztables.manuf_model',
             columns_and_aliases={'manufacturer_of_asset' : 'manufacturer', 
                                  'model_number' : 'model_number'})

def import_eqobjl(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    df = import_utils.read_xlsx_source(xlsx, normalize_column_names=True)
    import_utils.duckdb_write_dataframe_to_table(
            df, con=con, 
            qualified_table_name='s4_ztables.eqobjl',
            columns_and_aliases={'object_type': 'parent_objtype', 
                                 'object_type_1': 'child_objtype', 
                                 'equipment_category': 'equipment_category', 
                                 'remarks' : 'remarks'})

def import_flocdes(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    df = import_utils.read_xlsx_source(xlsx, normalize_column_names=True)
    import_utils.duckdb_write_dataframe_to_table(
            df, con=con, 
            qualified_table_name='s4_ztables.flocdes',
            columns_and_aliases={'object_type': 'objtype', 
                                 'standard_floc_description': 'standard_floc_description'})    


def import_floobjl(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    df = import_utils.read_xlsx_source(xlsx, normalize_column_names=True)
    import_utils.duckdb_write_dataframe_to_table(
            df, con=con, 
            qualified_table_name='s4_ztables.floobjl',
            columns_and_aliases={'structure_indicator': 'structure_indicator', 
                                 'object_type': 'parent_objtype', 
                                 'object_type_1': 'child_objtype', 
                                 'remarks': 'remarks'})   



def import_objtype_manuf(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    df = import_utils.read_xlsx_source(xlsx, normalize_column_names=True)
    import_utils.duckdb_write_dataframe_to_table(
            df, con=con, 
            qualified_table_name='s4_ztables.objtype_manuf',
            columns_and_aliases={'object_type': 'objtype', 
                                 'manufacturer_of_asset': 'manufacturer', 
                                 'remarks': 'remarks'})  
