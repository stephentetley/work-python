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

def copy_tables(*, classlists_source_db_path: str, con: duckdb.DuckDBPyConnection) -> None:
    sql = s4_classlists_table_copy(classlists_source_db_path=classlists_source_db_path)
    con.execute(sql)



def s4_classlists_table_copy(*, classlists_source_db_path: str) -> str: 
    return f"""
        ATTACH '{classlists_source_db_path}' AS classlists_source;
        INSERT INTO s4_classlists.characteristic_defs SELECT * FROM classlists_source.s4_classlists.characteristic_defs;
        INSERT INTO s4_classlists.enum_defs SELECT * FROM classlists_source.s4_classlists.enum_defs;
        DETACH classlists_source;
    """