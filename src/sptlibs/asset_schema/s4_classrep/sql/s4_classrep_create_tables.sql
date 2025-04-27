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

CREATE SCHEMA IF NOT EXISTS s4_classrep;


CREATE OR REPLACE TABLE s4_classrep.floc_masterdata (
    funcloc_id VARCHAR NOT NULL,
    functional_location VARCHAR NOT NULL,
    floc_description VARCHAR,
    internal_floc_ref VARCHAR,
    object_type VARCHAR,
    structure_indicator VARCHAR,
    superior_funct_loc VARCHAR,
    category VARCHAR,
    display_user_status VARCHAR,
    status_of_an_object VARCHAR,
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
    maint_work_center VARCHAR,
    work_center VARCHAR,
    planning_plant INTEGER,
    plant_section VARCHAR,
    object_number VARCHAR,
    floc_location VARCHAR,
    address_ref VARCHAR,
    PRIMARY KEY(funcloc_id)
);

CREATE OR REPLACE TABLE s4_classrep.equi_masterdata (
    equipment_id VARCHAR NOT NULL,
    equi_description VARCHAR NOT NULL,
    functional_location VARCHAR,
    superord_id VARCHAR,
    category VARCHAR,
    object_type VARCHAR,
    display_user_status VARCHAR,
    status_of_an_object VARCHAR,
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
    maint_work_center VARCHAR,
    work_center VARCHAR,
    planning_plant INTEGER,
    plant_section VARCHAR,
    equi_location VARCHAR,
    address_ref VARCHAR,
    PRIMARY KEY(equipment_id)
);

CREATE OR REPLACE TABLE s4_classrep.equi_longtext(
    equipment_id VARCHAR NOT NULL,
    long_text VARCHAR,
    PRIMARY KEY(equipment_id)
);



-- generate sql for class tables except EAST_NORTH, SOLUTION_ID and AIB_REFERENCE ...

-- ## AIB_REFERENCE (floc)


CREATE OR REPLACE TABLE s4_classrep.floc_aib_reference (
    funcloc_id VARCHAR NOT NULL,
    value_index INTEGER,
    ai2_aib_reference VARCHAR,
    PRIMARY KEY (funcloc_id, value_index)
);


-- ## AIB_REFERENCE (equi)



CREATE OR REPLACE TABLE s4_classrep.equi_aib_reference (
    equipment_id VARCHAR NOT NULL,
    value_index INTEGER,
    ai2_aib_reference VARCHAR,
    PRIMARY KEY (equipment_id, value_index)
);


-- ## ASSET_CONDITION

CREATE OR REPLACE TABLE s4_classrep.equi_asset_condition (
    equipment_id VARCHAR NOT NULL,
    condition_grade VARCHAR,
    condition_grade_reason VARCHAR,
    survey_comments VARCHAR,
    survey_date INTEGER,
    last_refurbished_date DATE,
    PRIMARY KEY(equipment_id)
);


-- ## EAST_NORTH

CREATE OR REPLACE TABLE s4_classrep.floc_east_north (
    funcloc_id VARCHAR NOT NULL,
    easting INTEGER,
    northing INTEGER,
    PRIMARY KEY(funcloc_id)
);


CREATE OR REPLACE TABLE s4_classrep.equi_east_north (
    equipment_id VARCHAR NOT NULL,
    easting INTEGER,
    northing INTEGER,
    PRIMARY KEY(equipment_id)
);



-- ## SOLUTION_ID

CREATE OR REPLACE TABLE s4_classrep.floc_solution_id (
    funcloc_id VARCHAR NOT NULL,
    value_index INTEGER,
    solution_id VARCHAR,
    PRIMARY KEY (funcloc_id, value_index)
);


CREATE OR REPLACE TABLE s4_classrep.equi_solution_id (
    equipment_id VARCHAR NOT NULL,
    value_index INTEGER,
    solution_id VARCHAR,
    PRIMARY KEY (equipment_id, value_index)
);


