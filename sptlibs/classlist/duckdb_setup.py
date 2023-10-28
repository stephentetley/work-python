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


s4_characteristic_defs_ddl = """
    CREATE OR REPLACE TABLE s4_characteristic_defs(
    class_type TEXT NOT NULL,
    class_name TEXT NOT NULL,
    char_name TEXT NOT NULL,
    class_description TEXT,
    char_description TEXT,
    char_type TEXT,
    char_length INTEGER,
    char_precision INTEGER,
    PRIMARY KEY(class_type, class_name, char_name)
    );
"""

s4_enum_defs_ddl = """
    -- Dont bother with primary key as it is a 4-tuple.
    CREATE OR REPLACE TABLE s4_enum_defs(
    class_type TEXT NOT NULL,
    class_name TEXT NOT NULL,
    char_name TEXT NOT NULL,
    enum_value TEXT NOT NULL,
    enum_description TEXT
    );
    """


def s4_characteristic_defs_insert(*, sqlite_path: str) -> str: 
    return f"""
    INSERT INTO s4_characteristic_defs
    SELECT 
        cc.class_type AS class_type,
        cc.class_name AS class_name,
        cc.char_name AS char_name,
        cc.class_description AS class_description,
        cc.char_description AS char_description,
        cc.char_type AS char_type,
        cc.char_length AS char_length,
        cc.char_precision AS char_precision
    FROM sqlite_scan('{sqlite_path}', 'classlist_characteristics') cc;
    """

def s4_enum_defs_insert(*, sqlite_path: str) -> str: 
    return f"""
    INSERT INTO s4_enum_defs
    SELECT 
        cev.class_type AS class_type,
        cev.class_name AS class_name,
        cev.char_name AS char_name,
        cev.enum_value AS enum_value,
        cev.enum_description AS enum_description
    FROM sqlite_scan('{sqlite_path}', 'classlist_enum_values') cev;
    """
