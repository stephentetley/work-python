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
        vs.value AS aib_ai2_reference,    
        sem.equi_id AS s4_equi_id
    FROM s4_equipment_master AS sem JOIN values_string vs 
        ON CAST(sem.equi_id AS TEXT) = vs.item_id
    WHERE
        vs.field_name = 'ai2_aib_reference'
    AND vs.value LIKE 'PLI%';


    CREATE OR REPLACE VIEW vw_equi_id_to_sai AS
    SELECT     
        sem.equi_id AS s4_equi_id, 
        vs.value AS aib_ai2_reference
    FROM s4_equipment_master AS sem JOIN values_string vs 
        ON CAST(sem.equi_id AS TEXT) = vs.item_id
    WHERE
        vs.field_name = 'ai2_aib_reference'
    AND vs.value LIKE 'SAI%';
    """

def telemetry_facts_insert(*, sqlite_path: str, sqlite_table: str) -> str: 
    return f"""
    INSERT INTO telemetry_facts
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
    INSERT INTO values_string
    SELECT 
        DISTINCT(spb.equipment) AS item_id,
        'ai2_aib_reference',
        spb.ai2_aib_reference AS value
    FROM sqlite_scan('{sqlite_path}', '{sqlite_table}') spb;

    """