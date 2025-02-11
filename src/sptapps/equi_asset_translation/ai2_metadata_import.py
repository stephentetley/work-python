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
import polars as pl
from sptlibs.utils.xlsx_source import XlsxSource
import sptlibs.data_access.import_utils as import_utils
from sptlibs.utils.sql_script_runner2 import SqlScriptRunner2



def duckdb_import(*, 
                  equipment_attributes_source: XlsxSource,
                  attribute_sets_source: XlsxSource,
                  con: duckdb.DuckDBPyConnection) -> None:
    runner = SqlScriptRunner2(__file__, con=con)
    runner.exec_sql_file(rel_file_path='ai2_metadata/setup_ai2_metadata.sql')
    df1 = _read_equipment_attributes_source(equipment_attributes_source)
    import_utils.duckdb_write_dataframe_to_table(df1,
                                                 qualified_table_name='ai2_metadata.equipment_attributes',
                                                 con=con)
    df2 = import_utils.read_xlsx_source(source=attribute_sets_source, 
                                        normalize_column_names=True)
    import_utils.duckdb_write_dataframe_to_table(df2,
                                                 qualified_table_name='ai2_metadata.attribute_sets',
                                                 con=con)
    


def _read_equipment_attributes_source(src: XlsxSource) -> pl.DataFrame: 
    df = pl.read_excel(source=src.path, 
                       sheet_name=src.sheet, 
                       engine='calamine', 
                       columns=['Code',
                                'Description', 
                                'AssetTypeDeletionFlag', 
                                'AttributeSet', 
                                'AttributeNameId', 
                                'Attribute Description',
                                'Attribute Name', 
                                'AttributeNameDeletionFlag',
                                'Unit Name', 
                                'Unit Description',
                                'Data Type Name', 
                                'Data Type Description'],
                       schema_overrides={'Code' : pl.String,
                                         'Description': pl.String, 
                                         'AssetTypeDeletionFlag': pl.Boolean,
                                         'AttributeSet': pl.String, 
                                         'AttributeNameId': pl.Int32, 
                                         'Attribute Description': pl.String,
                                         'Attribute Name': pl.String, 
                                         'AttributeNameDeletionFlag': pl.Boolean,
                                         'Unit Name': pl.String, 
                                         'Unit Description': pl.String,
                                         'Data Type Name': pl.String, 
                                         'Data Type Description': pl.String},
                       drop_empty_rows=True)
    df = df.rename({'Code' : 'asset_type_code',
                    'Description': 'asset_type_description', 
                    'AssetTypeDeletionFlag': 'asset_type_deletion_flag',
                    'AttributeSet': 'attribute_set', 
                    'AttributeNameId': 'attribute_name_id', 
                    'Attribute Description': 'attribute_description',
                    'Attribute Name': 'attribute_name', 
                    'AttributeNameDeletionFlag': 'attribute_name_deletion_flag',
                    'Unit Name': 'unit_name', 
                    'Unit Description': 'unit_description',
                    'Data Type Name': 'data_type_name', 
                    'Data Type Description': 'data_type_description'},
                    strict=True)
    return df



def copy_ai2_metadata_tables(*, source_db_path: str, dest_con: duckdb.DuckDBPyConnection) -> None:
    """`dest_con` is the desination database."""
    # copy tables using duckdb builtins
    import_utils.duckdb_import_tables_from_duckdb(source_db_path=source_db_path, 
                                                  con=dest_con,
                                                  schema_name='ai2_metadata',
                                                  create_schema=True,
                                                  source_tables=['ai2_metadata.equipment_attributes', 'ai2_metadata.attribute_sets'])
