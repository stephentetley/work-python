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

-- ai2 not synced
SELECT t.* EXCLUDE (aide_change, in_aide, to_be_translated),
FROM aide_triage.ai2_equipment_changes t
ANTI JOIN aide_triage.ih08_equi t1 ON t1.pli_number = t.ai2_ref
WHERE t.aide_change <> 'Child New'
AND t.to_be_translated = true
;

-- s4 not synced
SELECT t.* EXCLUDE (aide_change),
FROM aide_triage.ih08_equi t
WHERE t.pli_number IS NULL
;


-- s4 new (may include not synced items)
SELECT 
    t.* EXCLUDE (aide_change, in_aide, grid_ref, to_be_translated), 
    t.grid_ref AS osgb,
    t.ai2_ref AS ai2_pli_reference,
    t1.sai_num AS ai2_sai_reference,
    get_easting(osgb) AS easting,
    get_northing(osgb) AS northing,
FROM aide_triage.ai2_equipment_changes t
LEFT JOIN aide_triage.vw_ai2_parent_sai_nums t1 ON t1.pli_num = t.ai2_ref 
WHERE t.aide_change = 'Child New'
AND t.to_be_translated = true
;

-- s3 changes
SELECT 
    t1.equi_id AS equi_id,
    t.* EXCLUDE (in_aide, grid_ref, to_be_translated),
    t.grid_ref AS osgb,
    get_easting(osgb) AS easting,
    get_northing(osgb) AS northing,
FROM aide_triage.ai2_equipment_changes t
LEFT JOIN aide_triage.ih08_equi t1 ON t1.pli_number = t.ai2_ref
WHERE t.aide_change IN('Edit Relationship', 'Child Deleted')
AND t.to_be_translated = true
;
