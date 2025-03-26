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


-- All equipment must implement class AIB_REFERENCE

INSERT INTO asset_checking.checking_results BY NAME
WITH cte AS (
    SELECT 
        list(struct_pack(item := t.equipment_id, name := equi_description)) AS exceptions,
    FROM s4_classrep.equi_masterdata t
    ANTI JOIN s4_classrep.equi_aib_reference USING (equipment_id)
) 
SELECT 
    'error'::checker_serverity AS serverity,
    'Equipment Masterdata' AS category,
    'equi_implements_aib_reference' AS checker_name,
    'All equipment must implement the class AIB_REFERENCE' AS checker_description,
    t.exceptions AS checker_exceptions, 
FROM cte t
WHERE checker_exceptions IS NOT NULL
;


