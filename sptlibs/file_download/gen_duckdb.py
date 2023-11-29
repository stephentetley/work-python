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
import glob
from typing import Callable
import tempfile
import pandas as pd
import duckdb
import sptlibs.classlist.duckdb_setup as classlist_duckdb_setup
import sptlibs.classlist.duckdb_copy as classlist_duckdb_copy
import sptlibs.file_download.duckdb_setup as duckdb_setup
import sptlibs.file_download.load_file_download as load_file_download

class GenDuckdb:
    def __init__(self) -> None:
        self.output_directory = tempfile.gettempdir()
        self.db_name = 'file_downloads.duckdb'
        self.ddl_stmts = ['CREATE SCHEMA IF NOT EXISTS fd_raw;',
                            'CREATE SCHEMA IF NOT EXISTS s4_classlists;',
                            duckdb_setup.s4_fd_funcloc_masterdata_ddl,
                            duckdb_setup.s4_fd_equipment_masterdata_ddl,
                            duckdb_setup.s4_fd_classes_ddl, 
                            duckdb_setup.s4_fd_char_values_ddl,
                            classlist_duckdb_setup.s4_characteristic_defs_ddl,
                            classlist_duckdb_setup.s4_enum_defs_ddl,
                            ]
        self.insert_from_stmts = []
        self.copy_tables_stmts = []
        self.create_view_stmts = [classlist_duckdb_setup.vw_s4_class_defs_ddl,
                                    duckdb_setup.vw_entity_worklist_ddl,
                                    duckdb_setup.vw_fd_decimal_values_ddl,
                                    duckdb_setup.vw_fd_integer_values_ddl,
                                    duckdb_setup.vw_fd_text_values_ddl, 
                                    duckdb_setup.vw_fd_all_values_json_ddl,
                                    duckdb_setup.vw_fd_characteristics_json_ddl,
                                    duckdb_setup.vw_fd_classes_json_ddl,
                                    duckdb_setup.vw_get_classes_list_ddl,
                                    duckdb_setup.vw_get_class_name_ddl,
                                    duckdb_setup.vw_worklist_all_characteristics_json_ddl, 
                                    duckdb_setup.vw_worklist_all_classes_json_ddl
                                    ]
        self.imports = []

    def set_output_directory(self, *, output_directory: str) -> None: 
        self.output_directory = output_directory

    def add_file_download(self, *, path: str) -> None:
        self.imports.append((path))

    def add_file_downloads_in_directory(self, *, path: str, glob_pattern: str) -> None:
        globlist = glob.glob(glob_pattern, root_dir=path, recursive=False)
        for file_name in globlist: 
            self.imports.append(os.path.join(path, file_name))


    def add_classlist_tables(self, *, classlists_duckdb_path: str) -> None:
        stmt = classlist_duckdb_copy.s4_classlists_table_copy(classlists_duckdb_path=classlists_duckdb_path)
        self.copy_tables_stmts.append(stmt)


    def __add_insert_stmts(self, tables: list[str]) -> None:
        for table in tables:
            if table == 'funcloc_floc1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_funcloc_masterdata_insert)
            elif table == 'classfloc_classfloc1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_classfloc_insert)
            elif table == 'valuafloc_valuafloc1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_char_valuafloc_insert)
            elif table == 'equi_equi1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_equipment_masterdata_insert)
            elif table == 'classequi_classequi1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_classequi_insert)
            elif table == 'valuaequi_valuaequi1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_char_valuaequi_insert)
            else:
                print(f'Unrecognized file_download type {table}') 

    def _gen_raw_tables(self, *, con: duckdb.DuckDBPyConnection) -> None:
        tables = {}
        for path in self.imports:
            try:
                load_file_download.load_file_download(path=path, con=con)
            except Exception as exn:
                print(exn)
                continue


    def gen_duckdb(self) -> str:
        duckdb_outpath = os.path.normpath(os.path.join(self.output_directory, self.db_name))
        con = duckdb.connect(duckdb_outpath)
        for stmt in self.ddl_stmts:
            try:
                con.sql(stmt)
            except Exception as exn:
                print(exn)
                print(stmt)
                continue
        # Import raw data from download files
        self._gen_raw_tables(con=con)
        # Get raw tables...
        tables = []
        con.execute(duckdb_setup.query_get_raw_tables)
        for tup in con.fetchall():
            tables.append(tup[0])
        self.__add_insert_stmts(tables)
        # Copy from fd_raw tables
        for stmt in self.insert_from_stmts:
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
        for stmt in self.create_view_stmts:
            try:
                con.sql(stmt)
            except Exception as exn:
                print(exn)
                print(stmt)
                continue
        con.close()
        print(f'{duckdb_outpath} created')
        return duckdb_outpath
    


