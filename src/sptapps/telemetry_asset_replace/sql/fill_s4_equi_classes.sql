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

-- NETWTL
INSERT INTO s4_classrep.equiclass_netwtl BY NAME
SELECT 
	t1.fresh_number AS 'equipment_id',
	v1.attribute_value AS 'location_on_site',
FROM telemetry_landing.worklist t
JOIN asset_replace.vw_dollar_numbers t1 ON t1.ai2_reference = t.ai2_item_to_derive_new_equi_from
LEFT JOIN ai2_export.equi_eav_data v1 ON v1.ai2_reference = t.ai2_item_to_derive_new_equi_from AND v1.attribute_name = 'location_on_site'
;