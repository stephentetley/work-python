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
import sptapps.pointblue.pointblue_duckdb_setup as pointblue_duckdb_setup
from sptlibs.assets.gen_duckdb import GenDuckdb

class PointblueGenDuckdb(GenDuckdb):

    def __init__(self, *, sqlite_path: str, output_directory: str) -> None:
        GenDuckdb.__init__(self, sqlite_path=sqlite_path, output_directory=output_directory)
        self.ddl_stmts.append(pointblue_duckdb_setup.telemetry_facts_ddl)
        self.ddl_stmts.append(pointblue_duckdb_setup.id_mapping_views_ddl)

    def add_telemetry_facts_insert(self, *, sqlite_table: str) -> None:
        self.insert_from_stmts.append(pointblue_duckdb_setup.telemetry_facts_insert(sqlite_path=self.sqlite_src, sqlite_table=sqlite_table))

    def add_ai2_aib_reference_insert(self, *, sqlite_table: str) -> None:
        self.insert_from_stmts.append(pointblue_duckdb_setup.ai2_aib_reference_insert(sqlite_path=self.sqlite_src, sqlite_table=sqlite_table))

    def add_easting_northing_insert(self, *, sqlite_table: str) -> None:
        self.insert_from_stmts.append(pointblue_duckdb_setup.easting_insert(sqlite_path=self.sqlite_src, sqlite_table=sqlite_table))
        self.insert_from_stmts.append(pointblue_duckdb_setup.northing_insert(sqlite_path=self.sqlite_src, sqlite_table=sqlite_table))
