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

s4_ih_funcloc_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_funcloc_masterdata(
        funcloc_id TEXT NOT NULL,
        functional_location TEXT NOT NULL,
        address_ref INTEGER,
        category TEXT,
        company_code INTEGER,
        construction_month INTEGER,
        construction_year INTEGER,
        controlling_area INTEGER,
        cost_center INTEGER,
        description TEXT,
        display_position INTEGER,
        installation_allowed BOOLEAN,
        location TEXT,
        maintenance_plant INTEGER, 
        main_work_center TEXT,
        object_type TEXT,
        object_number TEXT,
        planning_plant INTEGER,
        plant_section TEXT,
        startup_date DATE,
        structure_indicator TEXT,
        superior_funct_loc TEXT,
        system_status TEXT,
        user_status TEXT,
        work_center TEXT,
        PRIMARY KEY(functional_location)
    );
"""


s4_ih_equipment_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_ih_equipment_masterdata(
        equi_id TEXT NOT NULL,
        address_ref INTEGER,
        catalog_profile TEXT,
        category TEXT,
        company_code INTEGER,
        construction_month INTEGER,
        construction_year INTEGER,
        controlling_area INTEGER,
        cost_center INTEGER,
        description TEXT,
        display_position INTEGER,
        functional_location TEXT, 
        gross_weight DECIMAL,
        location TEXT,
        main_work_center TEXT,
        maintenance_plant INTEGER,
        manufact_part_number TEXT,
        manufacturer TEXT,
        model_number TEXT,
        object_type TEXT,
        planning_plant INTEGER,
        plant_section TEXT,
        serial_number TEXT,
        startup_date DATE,
        superord_id TEXT,
        system_status TEXT,
        technical_ident_number TEXT,
        unit_of_weight TEXT,
        user_status TEXT,
        valid_from DATE,
        work_center TEXT,
        PRIMARY KEY(equi_id)
    );
"""

s4_ih_classes_ddl = """
    -- Same table for classequi and classfloc
    CREATE OR REPLACE TABLE s4_ih_classes(
        entity_id TEXT NOT NULL,
        class_name TEXT NOT NULL,
        class_type TEXT,
    );
    """

s4_ih_char_values_ddl = """
    -- No primary key
    CREATE OR REPLACE TABLE s4_ih_char_values(
        entity_id TEXT NOT NULL,
        class_type TEXT,
        char_name TEXT NOT NULL,
        char_text_value TEXT,
        char_numeric_value DECIMAL(26, 6)
    );
"""

s4_ih_equipment_masterdata_insert = """
    --- source table has _duplicates_ which cause an error without the row_number interior table 
    INSERT OR REPLACE INTO s4_ih_equipment_masterdata BY NAME
    SELECT 
        e.equipment AS equi_id,
        e.address_number AS address_ref,
        e.catalog_profile AS catalog_profile,
        e.equipment_category AS category,
        e.company_code AS company_code,
        e.construction_month AS construction_month,
        e.construction_year AS construction_year,
        e.controlling_area AS controlling_area,
        e.cost_center AS cost_center,
        e.description_of_technical_object AS description,
        e.functional_location AS functional_location,
        e.gross_weight AS gross_weight,
        e.location AS location,
        e.main_work_center AS main_work_center,
        e.maintenance_plant AS maintenance_plant,
        e.manufactserialnumber AS serial_number,
        e.manufacturer_part_number AS manufact_part_number,
        e.manufacturer_of_asset AS manufacturer,
        e.model_number AS model_number,
        e.object_type AS object_type,
        e.planning_plant AS planning_plant,
        e.plant_section AS plant_section,
        IF(e.position IS NULL, NULL, CAST(e.position AS INTEGER)) AS display_position,
        e.start_up_date::TIMESTAMP::DATE AS startup_date,
        e.superord_equipment AS superord_id,
        e.system_status AS system_status,
        e.technical_identification_no AS technical_ident_number,
        e.weight_unit AS unit_of_weight,
        e.user_status AS user_status,
        e.valid_from::TIMESTAMP::DATE AS valid_from,
        e.work_center AS work_center,
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY e1.equipment) AS rownum,
        FROM s4_raw_data.equi_masterdata e1
        ) e
    WHERE e.rownum = 1
"""

# OLD ...

s4_ih_char_values_ddl = """
    CREATE OR REPLACE TABLE s4_ih_char_values (
        entity_id TEXT NOT NULL,
        class_type TEXT NOT NULL,
        class_name TEXT NOT NULL,
        char_name TEXT NOT NULL,
        text_value TEXT,
        numeric_value DECIMAL(26,6)
    );
    """

vw_s4_classes_used_ddl = """
    CREATE OR REPLACE VIEW vw_s4_classes_used AS 
    SELECT 
        '002' AS class_type,
        upper(ddbt.table_name)[11:] AS class_name,
        ddbt.table_name AS table_name,
    FROM duckdb_tables() AS ddbt
    WHERE table_name LIKE 'valuaequi_%'
    UNION
    SELECT 
        '003' AS class_type,
        upper(ddbt.table_name)[11:] AS class_name,
        ddbt.table_name AS table_name,
    FROM duckdb_tables() AS ddbt
    WHERE table_name LIKE 'valuafloc_%';
    """
    
vw_characteristic_defs_with_type_ddl = """
    CREATE OR REPLACE VIEW vw_characteristic_defs_with_type AS 
    SELECT 
        scd.class_type AS class_type,
        scd.class_name AS class_name,
        scd.char_name AS char_name,
        regexp_replace(trim(regexp_replace(lower(scd.char_description), '[\W+]', ' ', 'g')), '[\W]+', '_', 'g') AS char_description,
        CASE 
            WHEN scd.char_type = 'CHAR' THEN 'TEXT'
            WHEN scd.char_type = 'DATE' THEN 'DATE'
            WHEN scd.char_type = 'NUM' THEN 'NUMERIC'
        END AS simple_data_type,
        CASE 
            WHEN scd.char_type = 'CHAR' THEN 'TEXT'
            WHEN scd.char_type = 'DATE' THEN 'DATE'
            WHEN scd.char_type = 'NUM' AND scd.char_precision = 0 THEN 'INTEGER'
            WHEN scd.char_type = 'NUM' AND scd.char_precision > 0 THEN format('DECIMAL({}, {})', scd.char_length, scd.char_precision)
        END AS ddl_data_type
    FROM s4_classlists.characteristic_defs scd;
    """

vw_s4_charateristics_used_ddl = """
    CREATE OR REPLACE VIEW vw_s4_charateristics_used AS
    SELECT 
        scu.class_type AS class_type, 
        scu.class_name AS class_name,
        cdwt.char_name AS char_name, 
        cdwt.char_description AS char_description, 
        cdwt.simple_data_type AS simple_data_type,
        cdwt.ddl_data_type AS ddl_data_type,
    FROM 
        vw_s4_classes_used scu
    JOIN vw_characteristic_defs_with_type cdwt ON cdwt.class_type = scu.class_type AND cdwt.class_name = scu.class_name;
    """