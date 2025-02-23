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

CREATE OR REPLACE VIEW aide_triage.vw_ai2_site_export_equi_names AS
WITH cte1 AS (
SELECT 
    t.reference, 
    t.common_name,
    regexp_extract(t.common_name, '(.*)/(EQUIPMENT:.*)', ['prefix', 'equitype']) AS name_struct,
    struct_extract(name_struct, 'prefix') AS prefix,
    struct_extract(name_struct, 'equitype') AS equi_type,
    length(prefix) AS prefix_len,
FROM raw_data.ai2_site_export t
WHERE t.common_name LIKE '%/EQUIPMENT:%'
), cte2 AS (
SELECT 
    t.reference, 
    t.common_name,
    t.prefix,
    t.equi_type,
    t1.common_name AS candidate,
FROM cte1 t
JOIN raw_data.ai2_site_export t1 ON starts_with(t.common_name, t1.common_name) AND length(t1.common_name) < t.prefix_len
), cte3 AS (
SELECT     
    t.reference, 
    t.common_name,
    max(t.candidate) AS longest_prefix,
    length(longest_prefix) AS pos,
    t.prefix[pos+2:] AS equi_name,
    t.equi_type AS equi_type,
FROM cte2 t
GROUP BY t.reference, t.common_name, t.prefix, t.equi_type
)
SELECT t.reference, t.common_name, t.equi_name, t.equi_type FROM cte3 t
ORDER BY common_name
;

CREATE OR REPLACE VIEW aide_triage.vw_ai2_parent_sai_nums AS
WITH cte1 AS (
SELECT 
    t.reference AS pli_num, 
    t.common_name,
    regexp_extract(t.common_name, '(.*)/(EQUIPMENT:.*)', ['prefix', 'equitype']) AS name_struct,
    struct_extract(name_struct, 'prefix') AS prefix,
    struct_extract(name_struct, 'equitype') AS equi_type,
    length(prefix) AS prefix_len,
FROM raw_data.ai2_site_export t
WHERE t.common_name LIKE '%/EQUIPMENT:%'
), cte2 AS (
SELECT 
    t.pli_num AS pli_num, 
    t1.reference AS sai_num,
    t.common_name AS common_name,
FROM cte1 t
JOIN raw_data.ai2_site_export t1 ON t1.common_name = t.prefix
) 
SELECT t.* FROM cte2 t
ORDER BY common_name
;


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
    t.object_type AS equi_type,
    t.functional_location AS functional_location,
    t.manufacturer_of_asset AS manufacturer,
    t.model_number AS model,
    t.manufacturer_part_number AS specific_model_frame,
    t.manufacturer_s_serial_number AS serial_number, 
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
FROM aide_triage.vw_ai2_site_export_equi_names t 
LEFT JOIN raw_data.ai2_site_export t1 ON t1.reference = t.reference 
LEFT JOIN raw_data.aide_changelist t2 ON t2.reference = t.reference 
LEFT JOIN equi_translation.equi_type_translation t3 ON t3.ai2_equi_type = t.equi_type 
;
