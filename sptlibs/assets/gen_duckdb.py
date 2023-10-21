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
import sptlibs.assets.duckdb_setup as duckdb_setup

class GenDuckdb:
    def __init__(self, *, sqlite_path: str, output_directory: str) -> None:
        self.db_name = 'assets.duckdb'
        self.sqlite_src = sqlite_path
        self.output_dir = output_directory
        self.ddl_stmts = [duckdb_setup.aib_equipment_master_ddl, duckdb_setup.s4_equipment_master_ddl, duckdb_setup.asset_values_ddl]
        self.insert_from_stmts = []

    def add_ddl_statement(self, ddl: str) -> None:
        self.ddl_stmts.append(ddl)

    def add_aib_worklist_ddl(self) -> None:
        self.ddl_stmts.append(duckdb_setup.aib_worklist_ddl)


    def add_s4_master_data_insert(self, *, sqlite_table: str) -> None:
        self.insert_from_stmts.append(duckdb_setup.s4_master_data_insert(sqlite_path=self.sqlite_src, sqlite_table=sqlite_table))

    def add_aib_master_data_insert(self, *, sqlite_table: str) -> None:
        self.insert_from_stmts.append(duckdb_setup.aib_master_data_insert(sqlite_path=self.sqlite_src, sqlite_table=sqlite_table))

    def add_aib_worklist_insert(self, *, sqlite_table: str) -> None:
        self.insert_from_stmts.append(duckdb_setup.aib_worklist_insert(sqlite_path=self.sqlite_src, sqlite_table=sqlite_table))

    def gen_duckdb(self) -> str:
        duckdb_outpath = os.path.join(self.output_dir, self.db_name)
        con = duckdb.connect(duckdb_outpath)
        for stmt in self.ddl_stmts:
            con.sql(stmt)
        for stmt in self.insert_from_stmts:
            con.sql(stmt)
        con.close()
        print(f'{duckdb_outpath} created')
        return duckdb_outpath
    


