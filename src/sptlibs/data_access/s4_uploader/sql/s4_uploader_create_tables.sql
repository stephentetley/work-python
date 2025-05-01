-- 
-- Copyright 2025 Stephen Tetley
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
-- http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- 


-- asset_schema_udfs must be loaded

CREATE SCHEMA IF NOT EXISTS s4_uploader;



-- The tables and views follow a pattern - the table contains just the fields 
-- a client needs to fill out, the view corresponds to a sheet in the uploader
-- xlsx file

-- IMPORTANT NOTE - we only model creating records with the `s4_uploader` tables,
-- we don't attempt to model updating records.

-- functional_location is just the fields client code needs to fill out...
CREATE OR REPLACE TABLE s4_uploader.functional_location (
    functional_location VARCHAR NOT NULL,
    description_medium VARCHAR NOT NULL,
    category INTEGER,
    object_type VARCHAR,
    start_up_date DATETIME,
    maint_plant INTEGER,
    display_user_status VARCHAR,
    PRIMARY KEY (functional_location)
);



CREATE OR REPLACE VIEW s4_uploader.vw_function_location_data AS
SELECT 
    t.functional_location AS functional_location,
    t.functional_location AS masked_func_loc,
    t.description_medium AS description_medium,
    t.category AS funct_loc_cat,
    'YW-GS' AS str_indicator,
    t.object_type AS object_type,
    null AS gross_weight,
    null AS unit_of_weight,
    strftime(t.start_up_date, '%d.%m.%Y') AS start_up_date,
    null AS currency,
    null AS acquistion_date,
    null AS construct_year,
    null AS construct_mth,
    2100 AS maint_plant,
    2100 AS company_code,
    1000 AS co_area,
    null AS planning_plant,
    null AS display_position,
    udfx.get_superior_floc(functional_location, funct_loc_cat) AS sup_funct_loc,
    IF(funct_loc_cat >= 5, 'X', '') AS equip_install,
    null AS status_of_an_object,
    null AS status_without_stsno,
FROM s4_uploader.functional_location t
ORDER BY functional_location;

-- equipment is just the fields client code needs to fill out...
CREATE OR REPLACE TABLE s4_uploader.equipment (
    equipment_id VARCHAR NOT NULL,
    description_medium VARCHAR NOT NULL,
    category VARCHAR,
    object_type VARCHAR,
    display_user_status VARCHAR,
    gross_weight_kg DECIMAL,
    start_up_date DATETIME,
    manufacturer VARCHAR,
    model VARCHAR,
    manuf_part_number VARCHAR,
    serial_number VARCHAR,
    functional_location VARCHAR,
    superord_equip VARCHAR,
    display_position INTEGER,
    tech_ident_no VARCHAR,
    status_of_an_object VARCHAR,
    maint_plant INTEGER,
    plant_section VARCHAR,
    work_center INTEGER,
    company_code INTEGER,
    co_area INTEGER,
    planning_plant INTEGER,
    maint_work_center VARCHAR,
    PRIMARY KEY (equipment_id)
);

-- TODO fill out null columns so shape matches uploader form...
CREATE OR REPLACE VIEW s4_uploader.vw_equipment_data AS
SELECT 
    t.maint_plant AS plant_work_center, 
    t.equipment_id AS equipment_id,
    t.category AS category,
    t.description_medium AS description_medium,
    strftime(current_date, '%d.%m.%Y') AS valid_from,
    IF(t.gross_weight_kg IS NULL, NULL, 'KG') AS unit_of_weight,
    t.gross_weight_kg AS gross_weight_kg,
    strftime(start_up_date, '%d.%m.%Y') AS start_up_date,
    manufacturer AS manufacturer,
    model AS model,
    manuf_part_number AS manuf_part_number,
    serial_number AS serial_number,
    strftime(start_up_date, '%Y') AS construct_year,
    strftime(start_up_date, '%m') AS construct_month,
    maint_plant AS maint_plant,
    t.work_center AS work_center,
    t.company_code AS company_code,
    t.co_area AS co_area,
    t.planning_plant AS planning_plant,
    t.maint_work_center AS maint_work_center,
    t.functional_location AS functional_location,
    t.superord_equip AS superord_equip,
    printf('%04d', t.display_position) AS display_position,
    t.tech_ident_no AS tech_ident_no,
    'ZEQUIPST' AS status_profile,
    t.status_of_an_object AS status_of_an_object,
FROM s4_uploader.equipment t
ORDER BY functional_location, equipment_id, superord_equip;


-- No Primary Key - multiples allowed
CREATE OR REPLACE TABLE s4_uploader.fl_classification (
    functional_location VARCHAR NOT NULL,
    class_name VARCHAR NOT NULL,
    characteristics VARCHAR NOT NULL,
    char_value VARCHAR,
);

CREATE OR REPLACE VIEW s4_uploader.vw_fl_classification AS
SELECT 
    t.functional_location AS functional_location,
    null AS deletion_ind,
   '003' AS class_type,
    t.class_name AS class_name,
    t.characteristics AS characteristics,
    t.char_value AS char_value,
    null AS ch_deletion_ind,
FROM s4_uploader.fl_classification t
ORDER BY functional_location, class_name, characteristics;


CREATE OR REPLACE TABLE s4_uploader.eq_classification (
    equipment_id VARCHAR NOT NULL,
    class_name VARCHAR NOT NULL,
    characteristics VARCHAR NOT NULL,
    char_value VARCHAR,
);

CREATE OR REPLACE VIEW s4_uploader.vw_eq_classification AS
SELECT 
    t.equipment_id AS equipment_id,
    null AS deletion_ind,
   '002' AS class_type,
    t.class_name AS class_name,
    t.characteristics AS characteristics,
    t.char_value AS char_value,
    null AS ch_deletion_ind,
FROM s4_uploader.eq_classification t
ORDER BY equipment_id, class_name, characteristics;



CREATE OR REPLACE VIEW s4_uploader.vw_change_request_details AS
WITH cte1 AS (
    (SELECT
        t.functional_location  AS functional_location,
        null AS equipment_id
    FROM s4_uploader.functional_location t)
    UNION
    (SELECT
        null AS functional_location,
        t.equipment_id AS equipment_id
    FROM s4_uploader.equipment t)
), cte2 AS (
    SELECT
          ROW_NUMBER() OVER (
          ORDER BY t.functional_location, t.equipment_id
          ) AS row_num,
          t.functional_location AS functional_location,
          t.equipment_id AS equipment_id,
    FROM
        cte1 t
)
SELECT 
    IF(cte2.row_num = 1, 'Upload Name', '') AS description_long,
    null AS priority,
    null AS due_date,
    null AS reason,
    IF(cte2.row_num = 1, 'AIWEAM0P', '') AS type_of_change_request,
    null AS change_request_group,
    null AS mbom_material,
    null AS mbom_plant,
    null AS mbom_usage,
    null AS mbom_alternative,
    cte2.functional_location AS fl_functional_location,
    cte2.equipment_id AS eq_equipment,
    'ASSET DATA' AS process_requester,
FROM cte2;

