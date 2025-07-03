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


CREATE OR REPLACE MACRO s4_classrep_to_excel_uploader.translate_equi_aib_reference() AS TABLE
SELECT 
    t.equipment_id AS 'equi',
    'AIB_REFERENCE' AS 'class_name',
    'AI2_AIB_REFERENCE' AS 'characteristics',
    t.ai2_aib_reference AS 'char_value'
FROM s4_classrep.equi_aib_reference t
;

CREATE OR REPLACE MACRO s4_classrep_to_excel_uploader.translate_equi_east_north() AS TABLE
SELECT 
    t.equipment_id AS 'equi',
    'EAST_NORTH' AS 'class_name',
    'EASTING' AS 'characteristics',
    t.easting AS 'char_value'
FROM s4_classrep.equi_east_north t
UNION BY NAME
SELECT 
    t.equipment_id AS 'equi',
    'EAST_NORTH' AS 'class_name',
    'NORTHING' AS 'characteristics',
    t.northing AS 'char_value'
FROM s4_classrep.equi_east_north t
;

CREATE OR REPLACE MACRO s4_classrep_to_excel_uploader.translate_equi_asset_condition() AS TABLE
SELECT 
    t.equipment_id AS 'equi',
    'ASSET_CONDITION' AS 'class_name',
    'CONDITION_GRADE' AS 'characteristics',
    t.condition_grade AS 'char_value'
FROM s4_classrep.equi_asset_condition t
WHERE t.condition_grade IS NOT NULL
UNION BY NAME
SELECT 
    t.equipment_id AS 'equi',
    'ASSET_CONDITION' AS 'class_name',
    'CONDITION_GRADE_REASON' AS 'characteristics',
    t.condition_grade_reason AS 'char_value'
FROM s4_classrep.equi_asset_condition t
WHERE t.condition_grade_reason IS NOT NULL
UNION BY NAME
SELECT 
    t.equipment_id AS 'equi',
    'ASSET_CONDITION' AS 'class_name',
    'LAST_REFURBISHED_DATE' AS 'characteristics',
    strftime(t.last_refurbished_date, '%d.%m.%Y') AS 'char_value'
FROM s4_classrep.equi_asset_condition t
WHERE t.last_refurbished_date IS NOT NULL
UNION BY NAME
SELECT 
    t.equipment_id AS 'equi',
    'ASSET_CONDITION' AS 'class_name',
    'SURVEY_COMMENTS' AS 'characteristics',
    t.survey_comments AS 'char_value'
FROM s4_classrep.equi_asset_condition t
WHERE t.survey_comments IS NOT NULL
UNION BY NAME
SELECT 
    t.equipment_id AS 'equi',
    'ASSET_CONDITION' AS 'class_name',
    'SURVEY_DATE' AS 'characteristics',
    printf('%d', t.survey_date) AS 'char_value'
FROM s4_classrep.equi_asset_condition t
WHERE t.survey_date IS NOT NULL
;


CREATE OR REPLACE MACRO s4_classrep_to_excel_uploader.translate_equiclass_netwtl() AS TABLE
SELECT 
    t.equipment_id AS 'equi',
    'NETWTL' AS 'class_name',
    'UNICLASS_CODE' AS 'characteristics',
    null AS 'char_value'
FROM s4_classrep.equiclass_netwtl t
UNION BY NAME 
SELECT 
    t.equipment_id AS 'equi',
    'NETWTL' AS 'class_name',
    'UNICLASS_DESC' AS 'characteristics',
    null AS 'char_value'
FROM s4_classrep.equiclass_netwtl t
UNION BY NAME 
SELECT 
    t.equipment_id AS 'equi',
    'NETWTL' AS 'class_name',
    'LOCATION_ON_SITE' AS 'characteristics',
    t.location_on_site AS 'char_value'
FROM s4_classrep.equiclass_netwtl t
WHERE t.location_on_site IS NOT NULL
;

INSERT INTO excel_uploader_equi_create.classification BY NAME
SELECT * FROM s4_classrep_to_excel_uploader.translate_equi_aib_reference()
UNION BY NAME 
SELECT * FROM s4_classrep_to_excel_uploader.translate_equi_east_north()
UNION BY NAME
SELECT * FROM s4_classrep_to_excel_uploader.translate_equi_asset_condition()
UNION BY NAME
SELECT * FROM s4_classrep_to_excel_uploader.translate_equiclass_netwtl()
ORDER BY equi, class_name, characteristics;