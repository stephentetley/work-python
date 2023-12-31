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

telemetry_facts_ddl = """
    CREATE OR REPLACE TABLE telemetry_facts(
        outstation_name TEXT NOT NULL, 
        site_sai_num TEXT,
        site_name TEXT,
        outstation_comment TEXT,
        outstation_id TEXT,
        outstation_pli_num TEXT,
        PRIMARY KEY(outstation_name)
    );
    """

id_mapping_views_ddl = """
    CREATE OR REPLACE VIEW vw_pli_to_equi_id AS
    SELECT 
        vt.text_value AS aib_ai2_reference,    
        sem.equi_id AS s4_equi_id
    FROM s4_equipment_master AS sem JOIN values_text vt 
        ON CAST(sem.equi_id AS TEXT) = vt.entity_id
    WHERE
        vt.attribute_name = 'AI2_AIB_REFERENCE'
    AND vt.text_value LIKE 'PLI%';


    CREATE OR REPLACE VIEW vw_equi_id_to_sai AS
    SELECT     
        sem.equi_id AS s4_equi_id, 
        vt.text_value AS aib_ai2_reference
    FROM s4_equipment_master AS sem JOIN values_text vt 
        ON CAST(sem.equi_id AS TEXT) = vt.entity_id
    WHERE
        vt.attribute_name = 'AI2_AIB_REFERENCE'
    AND vt.text_value LIKE 'SAI%';
    """

def telemetry_facts_insert(*, sqlite_path: str, sqlite_table: str) -> str: 
    return f"""
    INSERT INTO telemetry_facts BY NAME
    SELECT 
        tf.os_name AS outstation_name, 
        tf.od_name AS site_sai_num,
        tf.od_comment AS site_name,
        tf.os_comment AS outstation_comment,
        tf.cleansed_os_addr AS outstation_id,
        tf.ai2_pl_ref AS outstation_pli_num
    FROM sqlite_scan('{sqlite_path}', '{sqlite_table}') tf;
    """


def ai2_aib_reference_insert(*, sqlite_path: str, sqlite_table: str) -> str: 
    return f"""
    INSERT INTO values_text BY NAME
    SELECT 
        DISTINCT(spb.equipment) AS entity_id,
        'AI2_AIB_REFERENCE' AS attribute_name,
        spb.ai2_aib_reference AS text_value
    FROM sqlite_scan('{sqlite_path}', '{sqlite_table}') spb;
    """

def easting_insert(*, sqlite_path: str, sqlite_table: str) -> str: 
    return f"""
    INSERT INTO values_integer BY NAME
    SELECT 
        DISTINCT(CAST(spb.equipment AS TEXT)) AS entity_id,
        'EASTING' AS attribute_name,
        spb.easting AS integer_value
    FROM sqlite_scan('g:/work/2023/point_blue/point_blue_imports1.sqlite3', 's4_point_blue') spb
    WHERE spb.easting IS NOT NULL;
    """

def northing_insert(*, sqlite_path: str, sqlite_table: str) -> str: 
    return f"""
    INSERT INTO values_integer BY NAME
    SELECT 
        DISTINCT(CAST(spb.equipment AS TEXT)) AS entity_id,
        'NORTHING' AS attribute_name,
        spb.northing AS integer_value
    FROM sqlite_scan('g:/work/2023/point_blue/point_blue_imports1.sqlite3', 's4_point_blue') spb
    WHERE spb.northing IS NOT NULL;
    """