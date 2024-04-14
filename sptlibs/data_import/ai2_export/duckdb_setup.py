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

import duckdb

def setup_tables(*, con: duckdb.DuckDBPyConnection) -> None:
    con.execute('CREATE SCHEMA IF NOT EXISTS ai2_export;')
    con.execute(master_data_ddl)
    con.execute(eav_data_ddl)

master_data_ddl = """
    CREATE OR REPLACE TABLE ai2_export.master_data (
        ai2_reference VARCHAR NOT NULL,
        common_name VARCHAR NOT NULL,
        installed_from DATE,
        manufacturer VARCHAR,
        model VARCHAR,
        hierarchy_key VARCHAR,
        asset_status VARCHAR,
        asset_in_aide BOOLEAN,
        PRIMARY KEY(ai2_reference)
    );
"""

eav_data_ddl = """
    CREATE OR REPLACE TABLE ai2_export.eav_data(
        ai2_reference VARCHAR NOT NULL,
        attribute_name VARCHAR NOT NULL,
        attribute_value VARCHAR,
        PRIMARY KEY(ai2_reference, attribute_name)
    );
"""
