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
    CREATE OR REPLACE TABLE s4_classlists.characteristic_defs(
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
    CREATE OR REPLACE TABLE s4_classlists.enum_defs(
    class_type TEXT NOT NULL,
    class_name TEXT NOT NULL,
    char_name TEXT NOT NULL,
    enum_value TEXT NOT NULL,
    enum_description TEXT
    );
    """

vw_s4_class_defs_ddl = """
    CREATE OR REPLACE VIEW s4_classlists.vw_class_defs AS
    SELECT DISTINCT
        cd.class_type,
        cd.class_name,
        cd.class_description 
    FROM s4_classlists.characteristic_defs cd;
    """

vw_refined_characteristic_defs_ddl = """
    CREATE OR REPLACE VIEW s4_classlists.vw_refined_characteristic_defs AS
    SELECT 
        cd.class_type AS class_type, 
        cd.class_name AS class_name,
        cd.char_name AS char_name, 
        cd.class_description AS class_description,
        cd.char_description AS char_description,
        cd.char_type AS s4_char_type,
        cd.char_length AS char_len,
        cd.char_precision AS char_precision,
        CASE 
            WHEN cd.char_type = 'CHAR' THEN 'TEXT'
            WHEN cd.char_type = 'NUM' AND cd.char_precision = 0 THEN 'INTEGER'
            WHEN cd.char_type = 'NUM' AND cd.char_precision > 0 THEN 'DECIMAL'
            ELSE cd.char_type
        END AS refined_char_type,
        CASE 
            WHEN cd.char_type = 'CHAR' THEN format('VARCHAR({})', cd.char_length)
            WHEN cd.char_type = 'NUM' AND cd.char_precision = 0 THEN 'INTEGER'
            WHEN cd.char_type = 'NUM' AND cd.char_precision > 0 THEN format('DECIMAL({}, {})', cd.char_length, cd.char_precision)
            ELSE cd.char_type
        END AS ddl_data_type
    FROM s4_classlists.characteristic_defs cd;
    """

def df_s4_characteristic_defs_insert(*, dataframe_view: str) -> str: 
    return f"""
    INSERT INTO s4_classlists.characteristic_defs BY NAME
    SELECT 
        df.class_type AS class_type,
        df.class_name AS class_name,
        df.char_name AS char_name,
        df.class_description AS class_description,
        df.char_description AS char_description,
        df.char_type AS char_type,
        df.char_length AS char_length,
        IF(df.char_precision IS NULL, 0, df.char_precision) AS char_precision
    FROM {dataframe_view} df;
    """

def df_s4_enum_defs_insert(*, dataframe_view: str) -> str: 
    return f"""
    INSERT INTO s4_classlists.enum_defs BY NAME
    SELECT 
        df.class_type AS class_type,
        df.class_name AS class_name,
        df.char_name AS char_name,
        df.enum_value AS enum_value,
        df.enum_description AS enum_description
    FROM {dataframe_view} df;
    """

