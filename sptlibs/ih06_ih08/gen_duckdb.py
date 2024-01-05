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

import os
import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.classlist.duckdb_setup as classlist_duckdb_setup
import sptlibs.classlist.duckdb_copy as classlist_duckdb_copy
import sptlibs.ih06_ih08.load_raw_xlsx as load_raw_xlsx
    
class GenDuckdb:
    def __init__(self, *, classlists_duckdb_path: str, duckdb_output_path: str) -> None:
        self.classlists_source = classlists_duckdb_path
        self.duckdb_output_path = duckdb_output_path
        self.xlsx_ih06_imports = []
        self.xlsx_ih08_imports = []

    def add_ih06_export(self, src: XlsxSource) -> None:
        self.xlsx_ih06_imports.append(src)

    def add_ih08_export(self, src: XlsxSource) -> None:
        self.xlsx_ih08_imports.append(src)


    def gen_duckdb(self) -> str:
        '''Output DuckDB file with raw data.'''
        con = duckdb.connect(database=self.duckdb_output_path)
        
        # Create `s4_ihx_raw_data` tables
        con.execute('CREATE SCHEMA IF NOT EXISTS s4_ihx_raw_data;')

        # TODO properly account for multiple sheets / appending data
        # flocs
        for src in self.xlsx_ih06_imports:
            # equi and valuaequi tables
            load_raw_xlsx.load_ih06(xlsx_src=src, con=con)
        # equi
        for src in self.xlsx_ih08_imports:
            # equi and valuaequi tables
            load_raw_xlsx.load_ih08(xlsx_src=src, con=con)
        
        # Create `s4_classlists` tables
        if os.path.exists(self.classlists_source): 
            classlist_duckdb_setup.setup_tables(con=con)
            classlist_duckdb_copy.copy_tables(classlists_source_db_path=self.classlists_source, con=con)
        else:
            raise FileNotFoundError('classlist db not found')
        
        print(f'{self.duckdb_output_path} created')
        return self.duckdb_output_path

