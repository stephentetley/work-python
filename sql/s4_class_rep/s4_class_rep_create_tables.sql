-- 
-- Copyright 2024 Stephen Tetley
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

CREATE SCHEMA IF NOT EXISTS s4_class_rep;
CREATE SCHEMA IF NOT EXISTS s4_class_rep_staging;


CREATE OR REPLACE TABLE s4_class_rep.floc_master_data (
    floc_id VARCHAR NOT NULL,
    functional_location VARCHAR NOT NULL,
    description VARCHAR,
    object_type VARCHAR,
    structure_indicator VARCHAR,
    superior_funct_loc VARCHAR,
    category VARCHAR,
    user_status VARCHAR,
    system_status VARCHAR,
    installation_allowed BOOLEAN,
    startup_date DATE,
    construction_month INTEGER,
    construction_year INTEGER,
    display_position INTEGER,
    catalog_profile VARCHAR,
    company_code INTEGER,
    cost_center INTEGER,
    controlling_area INTEGER,
    maintenance_plant INTEGER,
    main_work_center VARCHAR,
    work_center VARCHAR,
    planning_plant INTEGER,
    plant_section VARCHAR,
    object_number VARCHAR,
    location VARCHAR,
    address_ref INTEGER,
    PRIMARY KEY(floc_id)
);

CREATE OR REPLACE TABLE s4_class_rep.equi_master_data (
    equipment_id VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    functional_location VARCHAR,
    superord_id VARCHAR,
    category VARCHAR,
    object_type VARCHAR,
    user_status VARCHAR,
    system_status VARCHAR,
    startup_date DATE,
    construction_month INTEGER,
    construction_year INTEGER,
    manufacturer VARCHAR,
    model_number VARCHAR,
    manufact_part_number VARCHAR,
    serial_number VARCHAR,
    gross_weight DECIMAL(18, 3),
    unit_of_weight VARCHAR,
    technical_ident_number VARCHAR,
    valid_from DATE,
    display_position INTEGER,
    catalog_profile VARCHAR,
    company_code INTEGER,
    cost_center INTEGER,
    controlling_area INTEGER,
    maintenance_plant INTEGER,
    main_work_center VARCHAR,
    work_center VARCHAR,
    planning_plant INTEGER,
    plant_section VARCHAR,
    location VARCHAR,
    address_ref INTEGER,
    PRIMARY KEY(equipment_id)
);

CREATE OR REPLACE TABLE s4_class_rep.equi_long_text(
    equipment_id VARCHAR NOT NULL,
    long_text VARCHAR,
    PRIMARY KEY(equipment_id)
);

-- generate sql for class tables except EAST_NORTH, SOLUTION_ID and AIB_REFERENCE ...

CREATE OR REPLACE TABLE s4_class_rep.equi_asset_condition (
    equipment_id VARCHAR NOT NULL,
    condition_grade VARCHAR,
    condition_grade_reason VARCHAR,
    survey_comments VARCHAR,
    survey_date INTEGER,
    last_refurbished_date DATE,
    PRIMARY KEY(equipment_id)
);

CREATE OR REPLACE VIEW s4_class_rep.vw_equisummary_asset_condition AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.description AS description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.manufact_part_number AS manufact_part_number,
    emd.serial_number AS serial_number,
    ea.* EXCLUDE (equipment_id),
FROM s4_class_rep.equi_asset_condition ea
JOIN s4_class_rep.equi_master_data emd ON emd.equipment_id = ea.equipment_id;


CREATE OR REPLACE TABLE s4_class_rep.floc_east_north (
    floc_id VARCHAR NOT NULL,
    easting INTEGER,
    northing INTEGER,
    PRIMARY KEY(floc_id)
);

CREATE OR REPLACE VIEW s4_class_rep.vw_equisummary_east_north AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.description AS description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.manufact_part_number AS manufact_part_number,
    emd.serial_number AS serial_number,
    ea.* EXCLUDE (equipment_id),
FROM s4_class_rep.equi_east_north ea
JOIN s4_class_rep.equi_master_data emd ON emd.equipment_id = ea.equipment_id;


CREATE OR REPLACE TABLE s4_class_rep.equi_east_north (
    equipment_id VARCHAR NOT NULL,
    easting INTEGER,
    northing INTEGER,
    PRIMARY KEY(equipment_id)
);

-- ## floc SOLUTION_ID

CREATE OR REPLACE TABLE s4_class_rep.floc_solution_id (
    floc_id VARCHAR NOT NULL,
    solution_ids VARCHAR[],
    PRIMARY KEY(floc_id)
);


-- ## equi SOLUTION_ID

CREATE OR REPLACE TABLE s4_class_rep.equi_solution_id (
    equipment_id VARCHAR NOT NULL,
    solution_ids VARCHAR[],
    PRIMARY KEY(equipment_id)
);


CREATE OR REPLACE VIEW s4_class_rep.vw_equisummary_solution_id AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.description AS description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.manufact_part_number AS manufact_part_number,
    emd.serial_number AS serial_number,
    ea.* EXCLUDE (equipment_id),
FROM s4_class_rep.equi_solution_id ea
JOIN s4_class_rep.equi_master_data emd ON emd.equipment_id = ea.equipment_id;


-- ## equi AIB_REFERENCE uses staging tables
-- We consider equipment as having principal SAI and PLI numbers. 
-- Equipment may also have a list of extra refernces which probably 
-- indicate data errors...

CREATE OR REPLACE TABLE s4_class_rep_staging.equi_ai2_sai_reference (
    equipment_id VARCHAR,
    value_index INTEGER,
    ai2_sai_reference VARCHAR,
    PRIMARY KEY(equipment_id)
);

CREATE OR REPLACE TABLE s4_class_rep_staging.equi_ai2_pli_reference (
    equipment_id VARCHAR,
    value_index INTEGER,
    ai2_pli_reference VARCHAR,
    PRIMARY KEY(equipment_id)
);


CREATE OR REPLACE VIEW s4_class_rep_staging.vw_equi_ai2_extra_references AS 
SELECT
    e.equipment_id AS equipment_id,
    array_agg(eav.atwrt) AS extra_aib_references,
FROM s4_class_rep.equi_master_data e
JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
ANTI JOIN s4_class_rep_staging.equi_ai2_sai_reference sai ON eav.atwrt = sai.ai2_sai_reference 
ANTI JOIN s4_class_rep_staging.equi_ai2_pli_reference pli ON eav.atwrt = pli.ai2_pli_reference 
WHERE eav.charid = 'AI2_AIB_REFERENCE'
GROUP BY equipment_id;

CREATE OR REPLACE VIEW s4_class_rep_staging.vw_equi_s4_aib_references AS
SELECT DISTINCT ON(e.equipment_id)
    e.equipment_id AS equipment_id,
    any_value(CASE WHEN eav.charid = 'S4_AIB_REFERENCE' THEN eav.atwrt ELSE NULL END) AS s4_aib_reference,
FROM s4_class_rep.equi_master_data e
JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
GROUP BY equipment_id;


CREATE OR REPLACE TABLE s4_class_rep.equi_aib_reference (
    equipment_id VARCHAR NOT NULL,
    sai_value_index INTEGER,
    ai2_sai_reference VARCHAR,
    pli_value_index INTEGER,
    ai2_pli_reference VARCHAR,
    ai2_extra_references VARCHAR[],
    s4_aib_reference VARCHAR,
    PRIMARY KEY(equipment_id)
);

CREATE OR REPLACE VIEW s4_class_rep.vw_equisummary_aib_reference AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.description AS description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.manufact_part_number AS manufact_part_number,
    emd.serial_number AS serial_number,
    ea.* EXCLUDE (equipment_id),
FROM s4_class_rep.equi_aib_reference ea
JOIN s4_class_rep.equi_master_data emd ON emd.equipment_id = ea.equipment_id;

-- ## TODO 

-- CREATE OR REPLACE TABLE s4_class_rep.floc_aib_reference (
--     floc_id VARCHAR NOT NULL,
--     index_solution_id INTEGER,
--     aib_reference VARCHAR,
-- );


-- ## stats tables

CREATE OR REPLACE VIEW s4_class_rep.vw_equiclass_stats AS
SELECT 
    array_slice(table_name, 11, len(table_name)) AS class_name,
    t.table_name AS table_name,
    t.estimated_size AS estimated_size, 
    t.estimated_size > 0 AS is_populated,
FROM duckdb_tables() t
WHERE 
    t.schema_name = 's4_class_rep'
AND t.table_name LIKE 'equiclass_%';

