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



def s4_classlists_table_copy(*, classlists_duckdb_path: str) -> str: 
    return f"""
        ATTACH '{classlists_duckdb_path}' AS classlists_db;
        CREATE SCHEMA IF NOT EXISTS s4_classlists;
        INSERT INTO s4_classlists.s4_characteristic_defs SELECT * FROM classlists_db.s4_classlists.characteristic_defs;
        INSERT INTO s4_classlists.s4_enum_defs SELECT * FROM classlists_db.s4_classlists.enum_defs;
        DETACH classlists_db;
    """