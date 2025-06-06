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




INSERT INTO aide_changes.ih08_equi BY NAME
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
    t.object_type AS equi_type,
    t.functional_location AS functional_location,
    t.manufacturer_of_asset AS manufacturer,
    t.model_number AS model,
    t.manufacturer_part_number AS specific_model_frame,
    t.manufacturer_s_serial_number AS serial_number, 
    t.user_status AS asset_status,
    t1.sai_number AS sai_number,
    t1.pli_number AS pli_number,
FROM raw_data.ih08_equi t
LEFT JOIN cte t1 ON t1.equi_id = t.equipment
LEFT JOIN raw_data.aide_changelist t2 ON t2.reference = t1.pli_number
;


INSERT INTO aide_changes.ai2_equipment_changes BY NAME
SELECT 
    t.reference AS ai2_ref,
    t2.change AS aide_change,
    t.equi_name AS equi_name,
    t.equi_type AS ai2_equi_type,
    IF(t3.is_translated = 'Not Translated', FALSE, TRUE) AS to_be_translated,
    t3.s4_category AS s4_category,
    t3.s4_obj_type AS s4_object_type,
    t3.s4_class AS s4_class,
    t.common_name AS common_name,
    t1.installed_from AS installed_from,
    t1.manufacturer AS manufacturer,
    t1.model AS model,
    t1.specific_model_frame AS specific_model_frame,
    t1.serial_no AS serial_number,
    t1.assetstatus AS asset_status,
    t1.loc_ref AS grid_ref,
    t1.asset_in_aide AS in_aide,
    t1.p_and_i_tag_no AS pandi_tag,
FROM raw_data.vw_ai2_site_export_equi_names t 
LEFT JOIN raw_data.ai2_site_export t1 ON t1.reference = t.reference 
LEFT JOIN raw_data.aide_changelist t2 ON t2.reference = t.reference 
LEFT JOIN equi_translation.equi_type_translation t3 ON t3.ai2_equi_type = t.equi_type 
;
