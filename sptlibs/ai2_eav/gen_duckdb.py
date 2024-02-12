"""
Copyright 2023 Stephen Tetley

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


def get_parent_data(df: pl.DataFrame) -> pl.DataFrame:
    df1 = df.select(pl.col("Reference", "Common Name"))
    df2 = df1.rename({"Reference" : "sai_num", "Common Name": "common_name"})
    return df2

    
class GenDuckdb:
    def __init__(self, *, duckdb_output_path: str) -> None:
        self.duckdb_output_path = duckdb_output_path
        self.xlsx_eav_sources = []
        self.xlsx_parents_source = None


    def add_parents_source(self, *, source: XlsxSource) -> None:
        self.xlsx_parents_source = source

    # def add_downloads_source_directory(self, *, source_dir: str, glob_pattern: str) -> None:
    #     globlist = glob.glob(glob_pattern, root_dir=source_dir, recursive=False)
    #     for file_name in globlist: 
    #         self.imports.append(os.path.join(source_dir, file_name))

    def gen_duckdb(self) -> str:
        con = duckdb.connect(database=self.duckdb_output_path)
        duckdb_setup.setup_tables(con=con)
        # parent_flocs...
        if self.xlsx_parents_source:
            polars_import_utils.duckdb_import_sheet(self.xlsx_parents_source, table_name='ai2_raw_data.parent_flocs', con=con, df_trafo=get_parent_data)
        con.close()
        print(f'{self.duckdb_output_path} created')
        return self.duckdb_output_path

