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

CREATE SCHEMA IF NOT EXISTS equi_asset_translation;



CREATE OR REPLACE TABLE equi_asset_translation.tt_equipment_classtypes (
    equipment_id VARCHAR NOT NULL,
    objtype VARCHAR NOT NULL,
    classtype VARCHAR NOT NULL,
    PRIMARY KEY(equipment_id)
);

CREATE OR REPLACE TABLE equi_asset_translation.mapping_worklist (
    equipment_id VARCHAR NOT NULL,
    common_name VARCHAR,
    ai2_equipment_type VARCHAR,
    s4_class VARCHAR,
    s4_name VARCHAR,
    PRIMARY KEY(equipment_id)
);


    


