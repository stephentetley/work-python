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


-- Level 5 flocs must specify SYSTEM_TYPE 

INSERT INTO asset_checking.checking_results BY NAME
WITH cte1 AS (
    SELECT 
        t.funcloc_id,
        t.functional_location,
        t1.atwrt
    FROM s4_classrep.floc_masterdata t
    JOIN file_download.valuafloc t1 ON t1.funcloc = t.funcloc_id 
    WHERE t.category = 5
    AND t1.charid = 'SYSTEM_TYPE'
), cte2 AS (
    SELECT 
        t.functional_location AS item_id,
        t.floc_description AS item_name,    
    FROM s4_classrep.floc_masterdata t 
    ANTI JOIN cte1 ON t.funcloc_id = cte1.funcloc_id
    WHERE t.category = 5
    AND t.object_type NOT IN ('ELF')
)
SELECT * FROM cte2
CROSS JOIN checker_classification(
        'warning'::checker_severity, 
        'Funcloc Masterdata/File Download', 
        'level5_flocs_must_have_system_type',
        'Level 5 flocs must define the value SYSTEM_TYPE in their respective system class'
        );