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

CREATE SCHEMA IF NOT EXISTS s4_uploader;

CREATE OR REPLACE MACRO get_superior_floc(floc, cat) AS 
    CASE 
        WHEN cat = 1 THEN ''
        WHEN cat = 2 THEN floc[:5]
        WHEN cat = 3 THEN floc[:9]
        WHEN cat = 4 THEN floc[:13]
        WHEN cat = 5 THEN floc[:17]
        WHEN cat = 6 THEN floc[:23] 
        ELSE '' 
    END;

-- The tables and views follow a pattern - the table contains just the fields 
-- a client needs to fill out, the view corresponds to a sheet in the uploader
-- xlsx file


-- functional_location is just the fields client code needs to fill out...
CREATE OR REPLACE TABLE s4_uploader.functional_location (
    functional_location VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    category INTEGER NOT NULL,
    object_type VARCHAR NOT NULL,
    start_up_date DATETIME,
    maint_plant INTEGER,
    user_status VARCHAR,
);



CREATE OR REPLACE VIEW s4_uploader.vw_function_location_data AS
SELECT 
    t.functional_location AS functional_location,
    t.functional_location AS masked_func_loc,
    t.description AS description,
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
    null AS position,
    get_superior_floc(functional_location, funct_loc_cat) AS sup_funct_loc,
    IF(funct_loc_cat >= 5, 'X', '') AS equip_install,
    null AS status_of_an_object,
    null AS status_without_stsno,
FROM s4_uploader.functional_location t
ORDER BY functional_location;

-- equipment is just the fields client code needs to fill out...
CREATE OR REPLACE TABLE s4_uploader.equipment (
    equipment_id VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    category INTEGER NOT NULL,
    object_type VARCHAR NOT NULL,
    start_up_date DATETIME,
    manufacturer VARCHAR,
    model VARCHAR,
    manuf_part_number VARCHAR,
    serial_number VARCHAR,
    functional_location VARCHAR NOT NULL,
    superord_equip VARCHAR,
    position VARCHAR,
    tech_ident_no VARCHAR,
    status_of_an_object VARCHAR,
    maint_plant INTEGER,
    user_status VARCHAR,
);

-- TODO - fill out...
CREATE OR REPLACE VIEW s4_uploader.vw_equipment_data AS
SELECT 
    t.equipment_id AS equipment_id,
    t.functional_location AS functional_location,
FROM s4_uploader.equipment t
ORDER BY functional_location, equipment_id;


-- No Primary Key - multiples allowed
CREATE OR REPLACE TABLE s4_uploader.fl_classification (
    functional_location VARCHAR NOT NULL,
    class VARCHAR NOT NULL,
    characteristics VARCHAR NOT NULL,
    char_value VARCHAR,
);

CREATE OR REPLACE VIEW s4_uploader.vw_fl_classification AS
SELECT 
    t.functional_location AS functional_location,
    null AS deletion_ind,
   '003' AS class_type,
    t.class AS class,
    t.characteristics AS characteristics,
    t.char_value AS char_value,
    null AS ch_deletion_ind,
FROM s4_uploader.fl_classification t
ORDER BY functional_location, class, characteristics;


CREATE OR REPLACE TABLE s4_uploader.eq_classification (
    equipment_id VARCHAR NOT NULL,
    class VARCHAR NOT NULL,
    characteristics VARCHAR NOT NULL,
    char_value VARCHAR,
);

CREATE OR REPLACE VIEW s4_uploader.vw_eq_classification AS
SELECT 
    t.equipment_id AS equipment_id,
    null AS deletion_ind,
   '002' AS class_type,
    t.class AS class,
    t.characteristics AS characteristics,
    t.char_value AS char_value,
    null AS ch_deletion_ind,
FROM s4_uploader.eq_classification t
ORDER BY equipment_id, class, characteristics;



CREATE OR REPLACE VIEW s4_uploader.vw_change_request_details AS
WITH cte AS (
    SELECT
      ROW_NUMBER() OVER (
      ORDER BY t.functional_location
      ) AS row_num,
      t.functional_location AS functional_location,
    FROM
      s4_uploader.functional_location t
)
SELECT 
    IF(cte.row_num = 1, 'Upload Name', '') AS description_long,
    null AS priority,
    null AS due_date,
    null AS reason,
    IF(cte.row_num = 1, 'AIWEAM0P', '') AS type_of_change_request,
    null AS change_request_group,
    null AS mbom_material,
    null AS mbom_plant,
    null AS mbom_usage,
    null AS mbom_alternative,
    cte.functional_location AS fl_functional_location,
    null AS eq_equipment,
    'ASSET DATA' AS process_requester,
FROM cte;

