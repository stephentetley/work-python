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

CREATE OR REPLACE MACRO extract_s4_equi_data_from_raw(table_name) AS TABLE 
SELECT DISTINCT ON (t."Equipment") 
    t."Equipment" AS equipment_id,
    t."Description of technical object" AS equi_description,
    t."Functional Location" AS functional_location,
    t."Functional Location"[1:5] AS s4_site,
    t."Technical identification no." AS techn_id_num,
    t."Equipment category" AS equi_category,
    t."Start-up date" AS startup_date,
    t."Manufacturer of Asset" AS manufacturer,
    t."Model number" AS model,
    t."Manufacturer part number" AS specific_model_frame,
    t."Manufacturer's Serial Number" AS serial_number,
    t."User Status" AS user_status,
    regexp_extract(user_status, '([A-Z]*)', 1) AS simple_status,
    t."Object Type" AS object_type,
    t1."AI2 AIB Reference" AS pli_num,
    t2."AI2 AIB Reference" AS sai_num,
FROM query_table(table_name::VARCHAR) t
LEFT JOIN query_table(table_name::VARCHAR) t1 ON t1."Equipment" = t."Equipment" AND t1."AI2 AIB Reference" LIKE 'PLI%'
LEFT JOIN query_table(table_name::VARCHAR) t2 ON t2."Equipment" = t."Equipment" AND (t2."AI2 AIB Reference" LIKE 'AFL%' OR t2."AI2 AIB Reference" LIKE 'SAI%')
ORDER BY functional_location
;

-- -- Calling example...
-- SELECT * FROM extract_s4_equi_data_from_raw(equi_raw_data.s4_export1)
-- UNION
-- SELECT * FROM extract_s4_equi_data_from_raw(equi_raw_data.s4_export2)
-- ;

CREATE OR REPLACE MACRO extract_ai2_equi_data_from_raw(table_name) AS TABLE 
WITH cte AS (
SELECT
    t."Reference" AS pli_num,
    t."Common Name" AS common_name,
    t."Installed From" AS startup_date,
    t."Manufacturer" AS manufacturer,
    t."Model" AS model,
    t."Specific Model/Frame" AS specific_model_frame,
    t."Serial No" AS serial_number,
    t."AssetStatus" AS user_status,
    t."Loc.Ref." AS grid_ref,
FROM query_table(table_name::VARCHAR) t
)
SELECT 
    t.*,
    t1.site AS installation_name,
    t2.s4_site_code AS s4_site_code,
    t1.item_name AS equi_name, 
    t1.equipment_type AS equipment_type,
FROM cte t
JOIN split_common_name(table_name::VARCHAR, 'Common Name') t1 ON t1.common_name = t.common_name
LEFT OUTER JOIN ai2_metadata.site_mapping t2 ON t2.installation_name = t1.site
;

-- -- Calling example...
-- SELECT * FROM extract_ai2_equi_data_from_raw(equi_raw_data.ai2_export1)
-- UNION
-- SELECT * FROM extract_ai2_equi_data_from_raw(equi_raw_data.ai2_export2)
-- UNION
-- SELECT * FROM extract_ai2_equi_data_from_raw(equi_raw_data.ai2_export3)
-- ;
