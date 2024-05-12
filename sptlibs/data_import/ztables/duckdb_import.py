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
from sptlibs.xlsx_source import XlsxSource
import sptlibs.data_import.import_utils as import_utils
import data_import.ztables._dbsetup as _dbsetup


def init(*, con: duckdb.DuckDBPyConnection) -> None: 
    _dbsetup.setup_tables(con=con)



def import_manuf_model(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO s4_ztables.manuf_model BY NAME
        SELECT 
            df.manufacturer_of_asset AS manufacturer,
            df.model_number AS model_number,
        FROM 
            df_dataframe_view df
        ;
        """
    import_utils.duckdb_import_sheet_into(xlsx, df_name='df_dataframe_view', insert_stmt=insert_stmt, df_trafo=None, con=con)


def import_eqobjl(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO s4_ztables.eqobjl BY NAME
        SELECT 
            df.object_type AS parent_objtype,
            df.object_type_duplicated_0 AS child_objtype,
            df.equipment_category AS equipment_category,
            df.remarks AS remarks,
        FROM 
            df_dataframe_view df
        ;
        """
    import_utils.duckdb_import_sheet_into(xlsx, df_name='df_dataframe_view', insert_stmt=insert_stmt, df_trafo=None, con=con)

def import_flocdes(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO s4_ztables.flocdes BY NAME
        SELECT 
            df.object_type AS objtype,
            df.standard_floc_description AS standard_floc_description,
        FROM 
            df_dataframe_view df
        ;
        """
    import_utils.duckdb_import_sheet_into(xlsx, df_name='df_dataframe_view', insert_stmt=insert_stmt, df_trafo=None, con=con)

def import_floobjl(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO s4_ztables.floobjl BY NAME
        SELECT 
            df.structure_indicator AS structure_indicator,
            df.object_type AS parent_objtype,
            df.object_type_duplicated_0 AS child_objtype,
            df.remarks AS remarks,
        FROM 
            df_dataframe_view df
        ;
        """
    import_utils.duckdb_import_sheet_into(xlsx, df_name='df_dataframe_view', insert_stmt=insert_stmt, df_trafo=None, con=con)


def import_objtype_manuf(xlsx: XlsxSource, *, con: duckdb.DuckDBPyConnection) -> None:
    insert_stmt = """
        INSERT INTO s4_ztables.objtype_manuf BY NAME
        SELECT 
            df.object_type AS objtype,
            df.manufacturer_of_asset AS manufacturer,
            df.remarks AS remarks,
        FROM 
            df_dataframe_view df
        ;
        """
    import_utils.duckdb_import_sheet_into(xlsx, df_name='df_dataframe_view', insert_stmt=insert_stmt, df_trafo=None, con=con)
