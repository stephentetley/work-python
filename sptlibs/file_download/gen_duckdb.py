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
import sptlibs.assets.duckdb_masterdata_ddl as duckdb_masterdata_ddl
import sptlibs.file_download.duckdb_setup as duckdb_setup
import sptlibs.classlist.duckdb_setup as classlist_duckdb_setup

class GenDuckdb:
    def __init__(self, *, sqlite_path: str, output_directory: str) -> None:
        self.db_name = 'file_downloads.duckdb'
        self.sqlite_src = sqlite_path
        self.output_dir = output_directory
        self.ddl_stmts = [duckdb_masterdata_ddl.s4_funcloc_masterdata_ddl,
                            duckdb_masterdata_ddl.s4_equipment_masterdata_ddl,
                            duckdb_setup.s4_fd_classes_ddl, 
                            duckdb_setup.s4_fd_char_values_ddl,
                            classlist_duckdb_setup.s4_characteristic_defs_ddl,
                            classlist_duckdb_setup.s4_enum_defs_ddl,
                            ]
        self.insert_from_stmts = []
        self.copy_tables_stmts = []
        self.create_view_stmts = [duckdb_setup.vw_fd_equi_decimal_values_ddl,
                                    duckdb_setup.vw_fd_equi_integer_values_ddl,
                                    duckdb_setup.vw_fd_equi_text_values_ddl
                                    ]

    def __add_insert_stmts(self, tables: list[str]) -> None:
        for table in tables:
            if table == 'funcloc_floc1':
                self.insert_from_stmts.append(duckdb_setup.s4_funcloc_masterdata_insert(sqlite_path=self.sqlite_src))
            elif table == 'classfloc_classfloc1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_classfloc_insert(sqlite_path=self.sqlite_src))
            elif table == 'valuafloc_valuafloc1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_char_valuafloc_insert(sqlite_path=self.sqlite_src))
            elif table == 'equi_equi1':
                self.insert_from_stmts.append(duckdb_setup.s4_equipment_masterdata_insert(sqlite_path=self.sqlite_src))
            elif table == 'classequi_classequi1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_classequi_insert(sqlite_path=self.sqlite_src))
            elif table == 'valuaequi_valuaequi1':
                self.insert_from_stmts.append(duckdb_setup.s4_fd_char_valuaequi_insert(sqlite_path=self.sqlite_src))
            else:
                print(f'Unrecognized file_download type {table}') 
                
    def add_classlist_tables(self, *, classlists_duckdb_path: str) -> None:
        self.copy_tables_stmts.append(duckdb_setup.s4_classlists_table_copy(classlists_duckdb_path=classlists_duckdb_path))

    def gen_duckdb(self) -> str:
        duckdb_outpath = os.path.normpath(os.path.join(self.output_dir, self.db_name))
        con = duckdb.connect(duckdb_outpath)
        # Get tables in SQLite...
        tables = []
        con.execute(duckdb_setup.query_sqlite_schema_tables(sqlite_path=self.sqlite_src))
        for tup in con.fetchall():
            tables.append(tup[0])
        self.__add_insert_stmts(tables)

        for stmt in self.ddl_stmts:
            try:
                con.sql(stmt)
            except Exception as exn:
                print(exn)
                print(stmt)
                continue
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
    


