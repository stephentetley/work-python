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


-- dispose of

CREATE OR REPLACE MACRO rename_as_del(equi_description) AS
    CASE 
    	WHEN len(equi_description) > 34 THEN (equi_description).replace('Telemetry', 'Telem').left(34) || ' (Del)'
    	ELSE equi_description || ' (Del)'
    END   
;

SELECT 
	t.s4_equipment_to_delete AS 'equipment',
	rename_as_del(t1.description_of_technical_object) AS 'description',
	t1.functional_location as 'functional_location',
	t1.user_status AS 'user_status',
FROM telemetry_landing.worklist t
JOIN ih08_landing.export1 t1 ON t1.equipment = t.s4_equipment_to_delete
WHERE t.s4_equipment_to_delete LIKE '10%'
AND t1.user_status LIKE 'OPER%';
