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

INSERT INTO s4_uploader.functional_location BY NAME
SELECT
    t.funcloc AS functional_location,
    t.floc_name AS description_medium,
    t.floc_category AS category,
    t.floc_type AS object_type,
    t.startup_date AS start_up_date,
    2110 AS maint_plant,
    t.user_status AS display_user_status,
FROM floc_delta.vw_new_flocs t;


-- SOLUTION_ID
INSERT INTO s4_uploader.fl_classification BY NAME
SELECT
    t.funcloc AS functional_location,
    'SOLUTION_ID' AS class_name,
    'SOLUTION_ID' AS characteristics,
    t.solution_id AS char_value,
FROM floc_delta.vw_new_flocs t
WHERE t.solution_id IS NOT NULL;

-- EASTING
INSERT INTO s4_uploader.fl_classification BY NAME
SELECT
    t.funcloc AS functional_location,
    'EAST_NORTH' AS class_name,
    'EASTING' AS characteristics,
    t.solution_id AS char_value,
FROM floc_delta.vw_new_flocs t
WHERE t.solution_id IS NOT NULL;


-- NORTHING
INSERT INTO s4_uploader.fl_classification BY NAME
SELECT
    t.funcloc AS functional_location,
    'EAST_NORTH' AS class_name,
    'NORTHING' AS characteristics,
    printf('%d', t.northing) AS char_value,
FROM floc_delta.vw_new_flocs t
WHERE t.northing IS NOT NULL;

-- Level 5 systems with SYSTEM_TYPE
INSERT INTO s4_uploader.fl_classification BY NAME
SELECT
    t.funcloc AS functional_location,
    t.floc_class AS class_name,
    'SYSTEM_TYPE' AS characteristics,
    t.level5_system_name AS char_value,
FROM floc_delta.vw_new_flocs t
WHERE t.level5_system_name IS NOT NULL;