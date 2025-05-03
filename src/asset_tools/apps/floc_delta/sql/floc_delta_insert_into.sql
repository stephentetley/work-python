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

CREATE SCHEMA IF NOT EXISTS floc_delta;

-- In progress - change to explicit DDL and INSERT INTO statements...

INSERT INTO floc_delta.worklist BY NAME (
SELECT 
    t."Requested Floc" AS requested_floc,
    t."Name" AS floc_description,
    t."Object Type" AS object_type,
    t."Class Type" AS class_type,
    t."Level 5 System Type" AS level5_system_name,
    t."Grid Ref" AS grid_ref,
    t."Solution ID" AS solution_id,
FROM floc_delta_landing.worklist t
);


INSERT INTO floc_delta.existing_flocs BY NAME (
SELECT 
    t.functional_location AS funcloc,
    t.description_of_functional_location AS floc_name,
    regexp_split_to_array(funcloc, '-').len() AS floc_category,
    t.start_up_date AS startup_date,
    TRY_CAST(t.cost_center AS INTEGER) AS cost_center,
    t.main_work_center AS maint_work_center,
    TRY_CAST(t.easting AS INTEGER) AS easting,
    TRY_CAST(t.northing AS INTEGER) AS northing,
FROM floc_delta_landing.floc_export_union t
);
    

INSERT INTO floc_delta.existing_and_new_flocs BY NAME (
WITH 
cte1 AS (
    SELECT 
        t.requested_floc AS floc, 
        t.floc_description AS source_name,
        t.object_type AS source_type,
        t.class_type AS source_class_type,
        regexp_split_to_array(floc, '-') AS arr,
        len(arr) as floc_category,
        list_extract(arr, 1) as site,
        list_extract(arr, 2) AS func,
        list_extract(arr, 3) AS proc_grp,
        list_extract(arr, 4) AS proc,
        list_extract(arr, 5) AS sysm,
        list_extract(arr, 6) AS subsysm,
        IF (func        IS NOT NULL, concat_ws('-', site, func), NULL) AS level2,
        IF (proc_grp    IS NOT NULL, concat_ws('-', site, func, proc_grp), NULL) AS level3,
        IF (proc        IS NOT NULL, concat_ws('-', site, func, proc_grp, proc), NULL) AS level4,
        IF (sysm        IS NOT NULL, concat_ws('-', site, func, proc_grp, proc, sysm), NULL) AS level5,
        IF (floc_category = 6,       concat_ws('-', site, func, proc_grp, proc, sysm, subsysm), NULL) AS level6,
    FROM floc_delta.worklist t
    ),
    cte2 AS (
        SELECT 
            t1.*,
            t2.standard_floc_description AS name_2,
            t3.standard_floc_description AS name_3,
            t4.standard_floc_description AS name_4,
        FROM cte1 t1
        LEFT JOIN s4_ztables.flocdes t2 ON t2.object_type = t1.func
        LEFT JOIN s4_ztables.flocdes t3 ON t3.object_type = t1.proc_grp
        LEFT JOIN s4_ztables.flocdes t4 ON t4.object_type = t1.proc
    )  
(SELECT  
    site AS funcloc,
    source_name AS floc_name,
    1 AS floc_category,
    'SITE' AS floc_type,
    NULL AS floc_class,
    NULL AS parent_floc,
FROM cte2 WHERE cte2.floc_category = 1)
UNION BY NAME
(SELECT 
    level2 AS funcloc,
    name_2 AS floc_name,
    2 AS floc_category,
    func AS floc_type,
    NULL AS floc_class,
    site AS parent_floc,
FROM cte2 WHERE cte2.level2 IS NOT NULL)
UNION BY NAME 
(SELECT 
    level3 AS funcloc,
    name_3 AS floc_name,
    3 AS floc_category,
    proc_grp AS floc_type,
    NULL AS floc_class,
    level2 AS parent_floc,
FROM cte2 WHERE cte2.level3 IS NOT NULL)
UNION BY NAME
(SELECT 
    level4 AS funcloc,
    name_4 AS floc_name,
    4 AS floc_category,
    proc AS floc_type,
    NULL AS floc_class,
    level3 AS parent_floc,
FROM cte2 WHERE cte2.level4 IS NOT NULL)
UNION BY NAME
(SELECT 
    level5 AS funcloc, 
    source_name AS floc_name, 
    5 AS floc_category,
    source_class_type AS floc_class,
    source_type AS floc_type, 
    level4 AS parent_floc,
FROM cte2 WHERE cte2.floc_category = 5)
UNION BY NAME
(SELECT 
    level6 AS funcloc, 
    source_name AS floc_name,
    6 AS floc_category,
    source_type AS floc_type, 
    NULL AS floc_class,
    level5 AS parent_floc,
FROM cte2 WHERE cte2.floc_category = 6)
ORDER BY funcloc
);


INSERT INTO floc_delta.new_generated_flocs BY NAME (
SELECT t1.* 
FROM floc_delta.existing_and_new_flocs t1
ANTI JOIN floc_delta.existing_flocs t2 ON t2.funcloc = t1.funcloc
ORDER BY funcloc
);



