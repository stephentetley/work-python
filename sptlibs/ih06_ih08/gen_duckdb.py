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
import tempfile
import duckdb
from sptlibs.xlsx_source import XlsxSource
import sptlibs.classlist.duckdb_setup as classlist_duckdb_setup
import sptlibs.classlist.duckdb_copy as classlist_duckdb_copy
import sptlibs.ih06_ih08.duckdb_setup as duckdb_setup
import sptlibs.ih06_ih08.load_raw_xlsx as load_raw_xlsx
import sptlibs.ih06_ih08.char_values as char_values
    
class GenDuckdb:
    def __init__(self) -> None:
        self.db_name = 'ih06_ih08.duckdb'
        self.output_directory = tempfile.gettempdir()
        self.xlsx_imports = []
        self.ddl_stmts = ['CREATE SCHEMA IF NOT EXISTS s4_classlists;',
                            classlist_duckdb_setup.s4_characteristic_defs_ddl,
                            classlist_duckdb_setup.s4_enum_defs_ddl, 
                            duckdb_setup.s4_ih_funcloc_masterdata_ddl,
                            duckdb_setup.s4_ih_equipment_masterdata_ddl,
                            duckdb_setup.s4_ih_classes_ddl,
                            duckdb_setup.s4_ih_char_values_ddl,
                            duckdb_setup.vw_s4_classes_used_ddl,
                            duckdb_setup.vw_characteristic_defs_with_type_ddl,
                            duckdb_setup.vw_s4_charateristics_used_ddl
                            ]
        self.copy_tables_stmts = []

    def set_output_directory(self, *, output_directory: str) -> None: 
        self.output_directory = output_directory

    def set_db_name(self, *, db_name: str) -> None:
        '''Just the name, not the path.'''
        self.db_name = db_name

    def add_ih06_export(self, src: XlsxSource) -> None:
        self.xlsx_imports.append(src)

    def add_ih08_export(self, src: XlsxSource) -> None:
        self.xlsx_imports.append(src)

    def add_classlist_tables(self, *, classlists_duckdb_path: str) -> None:
        self.copy_tables_stmts.append(classlist_duckdb_copy.s4_classlists_table_copy(classlists_duckdb_path=classlists_duckdb_path))

    def gen_duckdb(self) -> str:
        ''''''
        duckdb_outpath = os.path.normpath(os.path.join(self.output_directory, self.db_name))
        try:
            os.remove(duckdb_outpath)
        except OSError:
            pass
        con = duckdb.connect(database=duckdb_outpath)
        # Setup tables
        for stmt in self.ddl_stmts:
            try:
                con.sql(stmt)
            except Exception as exn:
                print(exn)
                print(stmt)
                continue
        for stmt in self.copy_tables_stmts:
            try:
                con.sql(stmt)
            except Exception as exn:
                print(exn)
                print(stmt)
                continue
        # TODO properly account for multiple sheets / appending data
        for src in self.xlsx_imports:
            # equi and valuaequi tables
            load_raw_xlsx.load_ih08(xlsx_src=src, con=con)
        char_values.make_vw_s4_valuaequi_eav(con=con)
        con.close()
        print(f'{duckdb_outpath} created')
        return duckdb_outpath

