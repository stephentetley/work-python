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


CREATE SCHEMA IF NOT EXISTS ai2_classrep;

CREATE OR REPLACE MACRO ai2_classrep.normalize_name(name) AS 
    '_' || lower(name).regexp_replace('[\W+]', ' ', 'g').trim().regexp_replace('[\W]+', '_', 'g');


CREATE OR REPLACE TABLE ai2_classrep.equi_masterdata (
    ai2_reference VARCHAR NOT NULL,
    common_name VARCHAR NOT NULL,
    item_name VARCHAR,
    equipment_type VARCHAR NOT NULL,
    installed_from DATE,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    asset_status VARCHAR,
    grid_ref VARCHAR,
    PRIMARY KEY (ai2_reference)
);



