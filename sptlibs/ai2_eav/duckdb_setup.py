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
    con.execute('CREATE SCHEMA IF NOT EXISTS ai2_raw_data;')
    con.execute(parent_flocs_ddl)
    con.execute(equipment_eav_ddl)


parent_flocs_ddl = """
    CREATE OR REPLACE TABLE ai2_raw_data.parent_flocs  (
        sai_num VARCHAR,
        common_name VARCHAR,
        PRIMARY KEY(sai_num)
    );
    """

equipment_eav_ddl = """
    CREATE OR REPLACE TABLE ai2_raw_data.equipment_eav  (
        sai_num VARCHAR,
        attr_name VARCHAR,
        attr_value VARCHAR,
        PRIMARY KEY(sai_num, attr_name)
    );
    """

