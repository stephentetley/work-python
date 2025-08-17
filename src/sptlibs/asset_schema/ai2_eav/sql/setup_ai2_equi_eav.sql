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


CREATE SCHEMA IF NOT EXISTS ai2_eav;

CREATE OR REPLACE TABLE ai2_eav.equipment_masterdata(
    ai2_reference VARCHAR,
    common_name VARCHAR,
    equipment_name VARCHAR,
    installed_from TIMESTAMP_MS,
    manufacturer VARCHAR,
    model VARCHAR,
    asset_status VARCHAR,
    loc_ref VARCHAR,
    PRIMARY KEY (ai2_reference)
);

CREATE OR REPLACE TABLE ai2_eav.equipment_eav(
    ai2_reference VARCHAR,
    attr_name VARCHAR,
    attr_value VARCHAR
);



