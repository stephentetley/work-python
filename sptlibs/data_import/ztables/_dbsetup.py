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


def setup_tables(*, con: duckdb.DuckDBPyConnection) -> None:
    con.execute('CREATE SCHEMA IF NOT EXISTS s4_ztables;')
    con.execute(manuf_model_ddl)
    con.execute(eqobjl_ddl)
    con.execute(flocdes_ddl)
    con.execute(floobjl_ddl)
    con.execute(objtype_manuf_ddl)


manuf_model_ddl = """
    CREATE OR REPLACE TABLE s4_ztables.manuf_model (
        manufacturer VARCHAR,
        model_number VARCHAR,
    );
"""

eqobjl_ddl = """
    CREATE OR REPLACE TABLE s4_ztables.eqobjl (
        parent_objtype VARCHAR,
        child_objtype VARCHAR,
        equipment_category VARCHAR,
        remarks VARCHAR,
    );
"""

flocdes_ddl = """
    CREATE OR REPLACE TABLE s4_ztables.flocdes (
        objtype VARCHAR,
        standard_floc_description VARCHAR,
    );
"""

floobjl_ddl = """
    CREATE OR REPLACE TABLE s4_ztables.floobjl (
        structure_indicator VARCHAR,
        parent_objtype VARCHAR,
        child_objtype VARCHAR,
        remarks VARCHAR,
    );
"""

objtype_manuf_ddl = """
    CREATE OR REPLACE TABLE s4_ztables.objtype_manuf (
        objtype VARCHAR,
        manufacturer VARCHAR,
        remarks VARCHAR,
    );
"""
