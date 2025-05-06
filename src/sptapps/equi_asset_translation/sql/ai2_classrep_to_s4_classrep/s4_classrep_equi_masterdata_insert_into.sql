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



-- NOTE - translate equiclasses first then we can use use 
-- equiclass_* to calculate classtype (objecttype and category) /old

-- TODO want to use `equi_asset_translation.worklist` instead...

INSERT OR REPLACE INTO s4_classrep.equi_masterdata BY NAME
SELECT 
    t.ai2_reference AS equipment_id,
    t.item_name AS equi_description,
    regexp_extract(t.common_name, '(.*)/EQUIPMENT:', 1) AS functional_location,   -- Fix this 
    null AS superord_id,
    'Z' AS category,
    t1.objtype AS object_type,
    udfx.get_s4_asset_status(t.asset_status) AS display_user_status,
    udfx.get_s4_asset_status(t.asset_status) AS status_of_an_object,
    TRY_CAST(t.installed_from AS DATE) AS startup_date,
    year(startup_date) AS construction_year,
    month(startup_date) AS construction_month,
    t.manufacturer AS manufacturer,
    t.model AS model_number,
    t.specific_model_frame AS manufact_part_number,
    t.serial_number AS serial_number,
    t.pandi_tag AS technical_ident_number,
FROM ai2_classrep.equi_masterdata t
LEFT JOIN equi_asset_translation.tt_equipment_classtypes t1 ON t1.equipment_id = t.ai2_reference
;

INSERT OR REPLACE INTO s4_classrep.equi_east_north BY NAME
SELECT 
    t.ai2_reference AS equipment_id,
    t1.easting AS easting,
    t1.northing AS northing,
FROM ai2_classrep.equi_masterdata t
CROSS JOIN udfx.get_east_north(t.grid_ref) t1
;

INSERT OR REPLACE INTO s4_classrep.equi_asset_condition BY NAME
SELECT 
    t.ai2_reference AS equipment_id,
    upper(t1.condition_grade) AS condition_grade,
    upper(t1.condition_grade_reason) AS condition_grade_reason,
    t1.survey_year AS survey_date,
FROM ai2_classrep.equi_masterdata t
LEFT JOIN ai2_classrep.equi_agasp t1 ON t1.ai2_reference = t.ai2_reference
