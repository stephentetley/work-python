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
import sptlibs.file_download.duckdb_setup as duckdb_setup

class GenDuckdb:
    def __init__(self, *, sqlite_path: str, output_directory: str) -> None:
        self.db_name = 'file_downloads.duckdb'
        self.sqlite_src = sqlite_path
        self.output_dir = output_directory
        self.ddl_stmts = [duckdb_setup.s4_fd_equi_ddl, duckdb_setup.s4_fd_classes_ddl, duckdb_setup.s4_fd_char_values_ddl]
        self.insert_from_stmts = [duckdb_setup.s4_fd_equi_insert(sqlite_path=self.sqlite_src),
                                     duckdb_setup.s4_fd_classes_insert(sqlite_path=self.sqlite_src),
                                     duckdb_setup.s4_fd_char_values_insert(sqlite_path=self.sqlite_src)]
        self.copy_tables_stmts = []


    def add_classlist_tables(self, *, classlists_duckdb_path: str) -> None:
        self.copy_tables_stmts.append(duckdb_setup.s4_classlists_table_copy(classlists_duckdb_path=classlists_duckdb_path))


    def gen_duckdb(self) -> str:
        duckdb_outpath = os.path.normpath(os.path.join(self.output_dir, self.db_name))
        con = duckdb.connect(duckdb_outpath)
        for stmt in self.ddl_stmts:
            con.sql(stmt)
        for stmt in self.insert_from_stmts:
            con.sql(stmt)
        for stmt in self.copy_tables_stmts:
            con.sql(stmt)
        con.close()
        print(f'{duckdb_outpath} created')
        return duckdb_outpath
    


