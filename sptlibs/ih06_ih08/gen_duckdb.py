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
import sptlibs.ih06_ih08.duckdb_setup as duckdb_setup

class GenDuckdb:
    def __init__(self, *, sqlite_path: str, output_directory: str) -> None:
        self.db_name = 'ih06_ih08_imports.duckdb'
        self.sqlite_src = sqlite_path
        self.output_dir = output_directory
        self.ddl_stmts = [duckdb_masterdata_ddl.s4_funcloc_masterdata_ddl,
                          duckdb_masterdata_ddl.s4_equipment_masterdata_ddl
                          ]
        self.insert_from_stmts = []

    def add_s4_equipment_master_insert(self, *, sqlite_table: str, has_aib_characteritics: bool) -> None:
        self.insert_from_stmts.append(duckdb_setup.s4_equipment_master_insert(sqlite_path=self.sqlite_src, 
                                                                                equi_tablename=sqlite_table,
                                                                                has_aib_characteritics=has_aib_characteritics))


    def add_s4_funcloc_master_insert(self, *, sqlite_table: str, has_aib_characteritics: bool) -> None:
        self.insert_from_stmts.append(duckdb_setup.s4_funcloc_master_insert(sqlite_path=self.sqlite_src, 
                                                                                floc_tablename=sqlite_table,
                                                                                has_aib_characteritics=has_aib_characteritics))
        
    def gen_duckdb(self) -> str:
        duckdb_outpath = os.path.normpath(os.path.join(self.output_dir, self.db_name))
        con = duckdb.connect(duckdb_outpath)
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
        con.close()
        print(f'{duckdb_outpath} created')
        return duckdb_outpath
    


