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



INSERT OR REPLACE INTO s4_uploader.equipment BY NAME
SELECT 
    t.equipment_id AS equipment_id,
    t.equi_description AS description_medium,
    t.category AS category,
    t.object_type AS object_type,
    t.display_user_status AS display_user_status,
    udfx.weight_to_kilograms(t.unit_of_weight, t.gross_weight) AS gross_weight_kg,
    t.startup_date AS start_up_date,
    t.manufacturer AS manufacturer,
    t.model_number AS model,
    t.manufact_part_number AS manuf_part_number,
    t.serial_number AS serial_number,
    t.functional_location AS functional_location,
    t.superord_id AS superord_equip,
    t.display_position AS display_position,
    t.technical_ident_number AS tech_ident_no,
    t.status_of_an_object AS status_of_an_object,
    t.maintenance_plant AS maint_plant,
    t.plant_section AS plant_section, 
    t.work_center AS work_center,
    t.company_code AS company_code,
    t.controlling_area AS co_area,
    t.planning_plant AS planning_plant,
    t.maint_work_center AS maint_work_center,
FROM s4_classrep.equi_masterdata t;

-- AIB_REFERENCE
INSERT INTO s4_uploader.eq_classification BY NAME 
(SELECT DISTINCT ON (equipment_id)
    t.equipment_id AS equipment_id,
    'AIB_REFERENCE' AS class,
    'S4_AIB_REFERENCE' AS characteristics,
    NULL AS char_value,
FROM s4_classrep.equi_aib_reference t)
UNION
(SELECT
    t.equipment_id AS equipment_id,
    'AIB_REFERENCE' AS class,
    'AI2_AIB_REFERENCE' AS characteristics,
    t.ai2_aib_reference AS char_value,
FROM s4_classrep.equi_aib_reference t);

-- ASSET_CONDITION (this is the template for all the equi_classes)
INSERT INTO s4_uploader.eq_classification BY NAME 
WITH cte AS (
    UNPIVOT s4_classrep.equi_asset_condition
    ON 
        condition_grade AS CONDITION_GRADE, 
        condition_grade_reason AS CONDITION_GRADE_REASON, 
        survey_comments AS SURVEY_COMMENTS, 
        printf('%d', survey_date) AS SURVEY_DATE, 
        printf('%d', last_refurbished_date) AS LAST_REFURBISHED_DATE,
    INTO
        NAME characteristics
        VALUE char_value
) SELECT *, 'ASSET_CONDITION' AS class FROM cte;

-- EAST_NORTH
INSERT INTO s4_uploader.eq_classification BY NAME 
WITH cte AS (
    UNPIVOT s4_classrep.equi_east_north
    ON 
        printf('%d', easting) AS EASTING, 
        printf('%d', northing) AS NORTHING,
    INTO
        NAME characteristics
        VALUE char_value
)
SELECT *, 'EAST_NORTH' AS class FROM cte;

-- SOLUTION_ID
INSERT INTO s4_uploader.eq_classification BY NAME 
SELECT
    t.equipment_id AS equipment_id,
    'SOLUTION_ID' AS class,
    'SOLUTION_ID' AS characteristics,
    t.solution_id AS char_value,
FROM s4_classrep.equi_solution_id t;