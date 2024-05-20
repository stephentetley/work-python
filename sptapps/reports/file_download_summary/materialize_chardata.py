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

def materialize_chardata(*, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(s4_summary_classvalues_insert)
    con.execute(s4_summary_charvalues_insert)


s4_summary_classvalues_insert = """
    INSERT INTO s4_summary.class_values BY NAME
    (SELECT DISTINCT 
        f.funcloc AS entity_id,
        f.class AS class_name,
        f.classtype AS class_type,
       FROM s4_fd_raw_data.classfloc_classfloc1 f)
    UNION 
    (SELECT DISTINCT 
        e.equi AS entity_id,
        e.class AS class_name,
        e.classtype AS class_type,
    FROM s4_fd_raw_data.classequi_classequi1 e)
"""

s4_summary_charvalues_insert = """
    INSERT INTO s4_summary.char_values BY NAME
    (SELECT 
        ce.equi AS entity_id,
        ce.classtype AS class_type,
        ce.class AS class_name,
        char_defs.char_name AS char_name,
        IF(char_defs.refined_char_type NOT IN ('INTEGER', 'DECIMAL'), ve.atwrt, NULL) AS char_text_value,
        IF(char_defs.refined_char_type = 'INTEGER', TRY_CAST(ve.atflv AS INTEGER), NULL) AS char_integer_value,
        IF(char_defs.refined_char_type = 'DECIMAL', TRY_CAST(ve.atflv AS DECIMAL), NULL) AS char_decimal_value,
    FROM 
        s4_fd_raw_data.classequi_classequi1 ce
    JOIN s4_classlists.vw_refined_characteristic_defs char_defs ON char_defs.class_type = ce.classtype AND char_defs.class_name = ce.class 
    LEFT OUTER JOIN s4_fd_raw_data.valuaequi_valuaequi1 ve ON ve.charid = char_defs.char_name AND ve.classtype = char_defs.class_type AND ve.equi = ce.equi)
    UNION
    (SELECT 
        cf.funcloc AS entity_id,
        cf.classtype AS class_type,
        cf.class AS class_name,
        char_defs.char_name AS char_name,
        IF(char_defs.refined_char_type NOT IN ('INTEGER', 'DECIMAL'), vf.atwrt, NULL) AS char_text_value,
        IF(char_defs.refined_char_type = 'INTEGER', TRY_CAST(vf.atflv AS INTEGER), NULL) AS char_integer_value,
        IF(char_defs.refined_char_type = 'DECIMAL', TRY_CAST(vf.atflv AS DECIMAL), NULL) AS char_decimal_value,
    FROM 
        s4_fd_raw_data.classfloc_classfloc1 cf
    JOIN s4_classlists.vw_refined_characteristic_defs char_defs ON char_defs.class_type = cf.classtype AND char_defs.class_name = cf.class 
    LEFT OUTER JOIN s4_fd_raw_data.valuafloc_valuafloc1 vf ON vf.charid = char_defs.char_name AND vf.classtype = char_defs.class_type AND vf.funcloc = cf.funcloc)
"""