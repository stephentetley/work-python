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

CREATE SCHEMA IF NOT EXISTS equi_raw_data;
CREATE SCHEMA IF NOT EXISTS equi_compare;


-- Includes calculated fields...
CREATE OR REPLACE TABLE equi_compare.ai2_equipment (
    pli_num VARCHAR NOT NULL,
    common_name VARCHAR,
    installation_name VARCHAR,
    equi_name VARCHAR,
    equipment_type VARCHAR,
    startup_date TIMESTAMP,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    pandi_tag VARCHAR,
    user_status VARCHAR,
    grid_ref VARCHAR,
    PRIMARY KEY (pli_num)
);

CREATE OR REPLACE TABLE equi_compare.s4_equipment (
    equipment_id VARCHAR NOT NULL,
    equi_description VARCHAR,
    functional_location VARCHAR,
    s4_site VARCHAR,
    techn_id_num VARCHAR,
    startup_date DATE,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    user_status VARCHAR,
    simple_status VARCHAR,
    object_type VARCHAR,
    address_id VARCHAR,
    pli_num VARCHAR,
    sai_num VARCHAR,
    PRIMARY KEY (equipment_id)
);

