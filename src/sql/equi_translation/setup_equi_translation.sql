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


CREATE SCHEMA IF NOT EXISTS ai2_landing;
CREATE SCHEMA IF NOT EXISTS ai2_eav;
CREATE SCHEMA IF NOT EXISTS ai2_classrep;
CREATE SCHEMA IF NOT EXISTS ai2_metadata;


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


-- Currently can't be invoked for qualified tables names, use `USE <schema>;` before invoking... 
CREATE OR REPLACE MACRO get_equipment_masterdata(table_name) AS TABLE
SELECT 
    reference AS ai2_reference,
    common_name AS common_name,
    regexp_extract(common_name, '(EQUIPMENT:.*)', 1) AS equipment_name,
    installed_from AS installed_from,
    manufacturer AS manufacturer,
    model AS model,
    assetstatus AS asset_status,
    loc_ref AS loc_ref,
FROM query_table(table_name::VARCHAR);

-- Currently can't be invoked for qualified tables names, use `USE <schema>;` before invoking...
CREATE OR REPLACE MACRO get_equipment_attr_values(table_name) AS TABLE
WITH cte1 AS (
    SELECT reference AS ai2_reference, 
    COLUMNS(* EXCLUDE (assetid, reference, common_name, installed_from, assetstatus, loc_ref, asset_in_aide, manufacturer, model, hierarchy_key))
    FROM query_table(table_name::VARCHAR)
), cte2 AS (
    SELECT cast(COLUMNS(*) AS VARCHAR)
    FROM cte1
)
UNPIVOT cte2
ON COLUMNS(* EXCLUDE (ai2_reference))
INTO
    NAME attr_name
    VALUE attr_value;



