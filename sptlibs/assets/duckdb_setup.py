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


s4_equipment_master_ddl = """
    CREATE OR REPLACE TABLE s4_equipment_master(
        equi_id TEXT NOT NULL,
        equi_name TEXT NOT NULL,
        func_loc TEXT,
        super_id TEXT,
        tag_name TEXT,
        asset_status TEXT,
        object_type TEXT,
        manufacturer TEXT,
        model TEXT,
        specific_model TEXT,
        serial_number TEXT,
        startup_date TIMESTAMP,
        equi_category TEXT,
        PRIMARY KEY(equi_id)
    );
"""
aib_equipment_master_ddl = """
    CREATE OR REPLACE TABLE aib_equipment_master(
        pli_num TEXT NOT NULL,
        common_name TEXT NOT NULL,
        installed_from TIMESTAMP,
        manufacturer TEXT,
        model TEXT,
        specific_model TEXT,
        serial_number TEXT,
        asset_status TEXT,
        PRIMARY KEY(pli_num)
    );
    """

asset_values_ddl = """
    -- No primary keys, (`item_id` * `field_name`) might not be unique

    CREATE OR REPLACE TABLE values_string(
        item_id TEXT NOT NULL,
        field_name TEXT NOT NULL, 
        field_value TEXT
    );

    CREATE OR REPLACE TABLE values_date(
        item_id TEXT NOT NULL,
        field_name TEXT NOT NULL, 
        field_value DATE
    );

    CREATE OR REPLACE TABLE values_time(
        item_id TEXT NOT NULL,
        field_name TEXT NOT NULL, 
        field_value TIME
    );

    CREATE OR REPLACE TABLE values_integer(
        item_id TEXT NOT NULL,
        field_name TEXT NOT NULL, 
        field_value INTEGER
    );


    CREATE OR REPLACE TABLE values_decimal(
        item_id TEXT NOT NULL,
        field_name TEXT NOT NULL, 
        field_value DECIMAL(18, 3)
    );

    CREATE OR REPLACE TABLE values_wide_decimal(
        item_id TEXT NOT NULL,
        field_name TEXT NOT NULL, 
        field_value DECIMAL(30, 8)
    );
    """

aib_worklist_ddl = """
    -- No primary key, `asset_id` might not be unique
    CREATE OR REPLACE TABLE aib_worklist(
        asset_id TEXT NOT NULL,
        submit_timestamp TIMESTAMP NOT NULL, 
        asset_name TEXT NOT NULL,
        status TEXT
    );
    """

def s4_master_data_insert(*, sqlite_path: str, sqlite_table: str) -> str: 
    return f"""
    INSERT INTO s4_equipment_master
    SELECT 
        DISTINCT(spb.equipment) AS equi_id,
        spb.description_of_technical_object AS equi_name,
        spb.functional_location AS func_loc,
        spb.superord_equipment AS super_id,
        spb.technical_identification_no AS tag_name,
        spb.user_status AS asset_status,
        spb.object_type AS object_type,
        spb.manufacturer_of_asset AS manufacturer,
        spb.model_number AS model,
        spb.manufacturer_part_number AS specific_model,
        spb.manufactserialnumber AS serial_number,
        spb.start_up_date AS startup_date,
        spb.equipment_category AS equi_category
    FROM sqlite_scan('{sqlite_path}', '{sqlite_table}') spb;
    """

def aib_master_data_insert(*, sqlite_path: str, sqlite_table: str) -> str: 
    return f"""
    INSERT INTO aib_equipment_master
    SELECT 
        DISTINCT(apb.reference) AS pli_num,
        apb.common_name AS common_name,
        apb.installed_from AS installed_from,
        apb.manufacturer AS manufacturer,
        apb.model AS model,
        apb.specific_model_frame AS specific_model,
        apb.serial_no AS serial_number,
        apb.assetstatus AS asset_status
    FROM sqlite_scan('{sqlite_path}', '{sqlite_table}') apb;
    """

def aib_worklist_insert(*, sqlite_path: str, sqlite_table: str) -> str: 
    return f"""
    INSERT INTO aib_worklist
    SELECT 
        w.asset_ref AS asset_id,
        w.date AS submit_timestamp, 
        w.assetname AS asset_name,
        w.status AS status
    FROM sqlite_scan('{sqlite_path}', '{sqlite_table}') w;
    """
