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


INSERT OR IGNORE INTO ai2_classrep.equi_masterdata BY NAME
SELECT 
    t1.ai2_reference AS ai2_reference,
    t1.common_name AS common_name,
    'TODO' AS item_name,
    t1.equipment_name AS equipment_type,
    (t1.installed_from:: DATE) AS installed_from,
    t1.manufacturer AS manufacturer,
    t1.model AS model,
    t1.asset_status AS asset_status,
    t1.loc_ref AS grid_ref,
FROM ai2_eav.equipment_masterdata AS t1
;