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
import sptlibs.ai2_eav.duckdb_setup as duckdb_setup
from sptlibs.xlsx_source import XlsxSource
import sptlibs.polars_import_utils as polars_import_utils


class GenDuckdb:
    def __init__(self, *, duckdb_output_path: str) -> None:
        self.duckdb_output_path = duckdb_output_path
        self.xlsx_eav_sources = []
        self.xlsx_parents_source = None


    def add_parents_source(self, *, source: XlsxSource) -> None:
        self.xlsx_parents_source = source

    def add_eav_source(self, *, source: XlsxSource) -> None:
        self.xlsx_eav_sources.append(source)

    def gen_duckdb(self) -> str:
        con = duckdb.connect(database=self.duckdb_output_path)
        duckdb_setup.setup_tables(con=con)
        # parent_flocs...
        if self.xlsx_parents_source:
            polars_import_utils.duckdb_import_sheet(self.xlsx_parents_source, table_name='ai2_raw_data.parent_flocs', con=con, df_trafo=_get_parent_data)
        # entity attribute values... primary (equipment) attributes first
        if self.xlsx_eav_sources:
            psrc = self.xlsx_eav_sources[0]
            primary_df = pl.read_excel(source=psrc.path, sheet_name=psrc.sheet)
            primary_df1 = _melt_primary_attributes(primary_df)
            con.register(view_name='vw_primary_df', python_object=primary_df1)
            con.execute(duckdb_setup.equipment_eav_insert(df_view_name="vw_primary_df"))
        # next characteristic attributes...
        for src in self.xlsx_eav_sources:
            df = pl.read_excel(source=src.path, sheet_name=src.sheet)
            df1 = _melt_attributes(df)
            con.register(view_name='vw_df', python_object=df1)
            con.execute(duckdb_setup.equipment_eav_insert(df_view_name="vw_df"))
        
        con.close()
        print(f'{self.duckdb_output_path} created')
        return self.duckdb_output_path
        

columns_to_drop = [
    'AssetId', 'Common Name', 'Installed From', 'Manufacturer', 'Model', 
    'Hierarchy Key', 'AssetStatus', 'Loc.Ref.', 'Asset in AIDE ?'
]

primary_columns = [
    'Reference', 'Common Name', 'Installed From', 'Manufacturer', 'Model', 
    'Hierarchy Key', 'AssetStatus', 'Loc.Ref.', 'Asset in AIDE ?'
]

def _get_parent_data(df: pl.DataFrame) -> pl.DataFrame:
    df1 = df.select(pl.col("Reference", "Common Name"))
    df2 = df1.rename({"Reference" : "sai_num", "Common Name": "common_name"})
    return df2


def _melt_attributes(df: pl.DataFrame) -> pl.DataFrame:
    df1 = df.drop(columns_to_drop)
    pivot_cols = df1.columns.remove("Reference") 
    df2 = df1.melt(id_vars="Reference", value_vars=pivot_cols)
    return df2


def _melt_primary_attributes(df: pl.DataFrame) -> pl.DataFrame:
    df1 = df.select(primary_columns)
    pivot_cols = df1.columns.remove("Reference") 
    df2 = df1.melt(id_vars="Reference", value_vars=pivot_cols)
    return df2
    