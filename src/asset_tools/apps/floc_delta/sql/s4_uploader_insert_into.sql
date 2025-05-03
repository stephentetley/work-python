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
    current_date AS start_up_date,
    2110 AS maint_plant,
    'OPER' AS display_user_status,
FROM floc_delta.new_generated_flocs t
;
