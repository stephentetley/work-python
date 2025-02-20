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

CREATE SCHEMA IF NOT EXISTS aide_triage;


CREATE OR REPLACE TABLE aide_triage.ih08_equi AS
WITH cte AS (
    SELECT DISTINCT 
        t.equipment AS equi_id,
        t1.ai2_aib_reference AS sai_number,
        t2.ai2_aib_reference AS pli_number,
    FROM raw_data.ih08_equi t
    LEFT JOIN raw_data.ih08_equi t1 ON t1.equipment = t.equipment AND t1.ai2_aib_reference LIKE 'SAI%'
    LEFT JOIN raw_data.ih08_equi t2 ON t2.equipment = t.equipment AND t2.ai2_aib_reference LIKE 'PLI%'
)
SELECT DISTINCT 
    t.equipment AS equi_id,
    t2.change AS aide_change,
    t.description_of_technical_object AS description,
    t.functional_location AS functional_location,
    t.manufacturer_of_asset AS manufacturer,
    t.model_number AS model,
    t.manufacturer_part_number AS specific_model_frame,
    t.manufacturer_s_serial_number AS serial_number, 
    t.object_type AS equi_type,
    t.user_status AS status,
    t1.sai_number AS sai_number,
    t1.pli_number AS pli_number,
FROM raw_data.ih08_equi t
LEFT JOIN cte t1 ON t1.equi_id = t.equipment
LEFT JOIN raw_data.aide_changelist t2 ON t2.reference = t1.pli_number
;


CREATE OR REPLACE TABLE aide_triage.ai2_equipment_changes AS
SELECT 
    t.reference AS ai2_ref,
    t1.change AS aide_change,
    t.common_name AS common_name,
    t.installed_from AS installed_from,
    t.manufacturer AS manufacturer,
    t.model AS model,
    t.specific_model_frame AS specific_model_frame,
    t.serial_no AS serial_number,
    t.assetstatus AS asset_status,
    t.loc_ref AS grid_ref,
    t.asset_in_aide AS in_aide,
    t.p_and_i_tag_no AS pandi_tag,
FROM raw_data.ai2_site_export t
LEFT JOIN raw_data.aide_changelist t1 ON t1.reference = t.reference 
WHERE t.common_name LIKE '%/EQUIPMENT:%'
;