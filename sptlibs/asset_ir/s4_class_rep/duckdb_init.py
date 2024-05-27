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
import sptlibs.asset_ir.s4_class_rep._dbsetup as _dbsetup
import sptlibs.asset_ir.s4_class_rep._materialize_masterdata as _materialize_masterdata

def init_s4_class_rep_tables(*, con: duckdb.DuckDBPyConnection) -> None: 
    _dbsetup.setup_tables(con=con)

def materialize_data(*, con: duckdb.DuckDBPyConnection) -> None:
    _materialize_masterdata.materialize_masterdata(con=con)
    