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
    con.execute('CREATE SCHEMA IF NOT EXISTS ai2_to_s4;')
    con.execute(vw_parent_sai_numbers_ddl)

vw_parent_sai_numbers_ddl = """
    CREATE OR REPLACE VIEW ai2_to_s4.vw_parent_sai_numbers AS
    SELECT 
        equi.sai_num AS pli_num,
        equi.attr_value AS equi_common_name,
        parent.sai_num AS sai_num,
    FROM
        ai2_raw_data.equipment_eav equi
    JOIN ai2_raw_data.parent_flocs parent ON instr(equi.attr_value, parent.common_name) > 0
    WHERE 
        equi.attr_name = 'Common Name'
    ;
"""
