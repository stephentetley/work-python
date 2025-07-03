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

-- AIB_REFERENCE
INSERT INTO s4_classrep.equi_aib_reference BY NAME
WITH cte AS (
SELECT 
	t1.fresh_number AS 'equipment_id',
	t.ai2_item_to_derive_new_equi_from AS pli_reference,
	t3.reference AS sai_reference,
FROM telemetry_landing.worklist t
JOIN asset_replace.vw_dollar_numbers t1 ON t1.ai2_reference = t.ai2_item_to_derive_new_equi_from
LEFT JOIN ai2_export_landing.export1 t2 ON t2.reference = t.ai2_item_to_derive_new_equi_from
LEFT JOIN ai2_export_landing.export2 t3 ON starts_with(t2.common_name, t3.common_name)
)
(SELECT 
	equipment_id AS equipment_id,
	1 AS value_index,
	pli_reference AS ai2_aib_reference,
FROM cte)
UNION
(SELECT 
	equipment_id AS equipment_id,
	2 AS value_index,
	sai_reference AS ai2_aib_reference,
FROM cte)
;

INSERT INTO s4_classrep.equi_east_north BY NAME
SELECT 
	t1.fresh_number AS 'equipment_id',
    t3.easting AS easting,
    t3.northing AS northing,
FROM telemetry_landing.worklist t
JOIN asset_replace.vw_dollar_numbers t1 ON t1.ai2_reference = t.ai2_item_to_derive_new_equi_from
LEFT JOIN ai2_export_landing.export1 t2 ON t2.reference = t.ai2_item_to_derive_new_equi_from
CROSS JOIN udfx.get_east_north(t2.loc_ref) t3
;

-- ASSET_CONDITION
INSERT INTO s4_classrep.equi_asset_condition BY NAME
SELECT 
	t1.fresh_number AS 'equipment_id',
	'1 - GOOD' AS condition_grade,
	'NEW' AS condition_grade_reason,
	extract('year' FROM t2.installed_from) AS survey_date,
FROM telemetry_landing.worklist t
JOIN asset_replace.vw_dollar_numbers t1 ON t1.ai2_reference = t.ai2_item_to_derive_new_equi_from
LEFT JOIN ai2_export_landing.export1 t2 ON t2.reference = t.ai2_item_to_derive_new_equi_from
;


-- NETWTL
INSERT INTO s4_classrep.equiclass_netwtl BY NAME
SELECT 
	t1.fresh_number AS 'equipment_id',
	v1.attribute_value AS 'location_on_site',
FROM telemetry_landing.worklist t
JOIN asset_replace.vw_dollar_numbers t1 ON t1.ai2_reference = t.ai2_item_to_derive_new_equi_from
LEFT JOIN ai2_export.equi_eav_data v1 ON v1.ai2_reference = t.ai2_item_to_derive_new_equi_from AND v1.attribute_name = 'location_on_site'
;

