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


-- All equipment must implement class EAST_NORTH

INSERT INTO asset_checking.checking_results BY NAME
WITH cte AS (
    SELECT 
        t.equipment_id AS item_id,
        t.equi_description AS item_name,
    FROM s4_classrep.equi_masterdata t
    ANTI JOIN s4_classrep.equi_east_north USING (equipment_id)
) 
SELECT * FROM cte
CROSS JOIN checker_classification(
        'error'::checker_severity, 
        'Equipment Masterdata', 
        'equi_implements_east_north',
        'All equipment must implement the class EAST_NORTH'
        );


