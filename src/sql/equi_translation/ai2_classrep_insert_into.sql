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



INSERT OR IGNORE INTO ai2_classrep.equi_masterdata BY NAME
SELECT 
    t1.ai2_reference AS ai2_reference,
    t1.common_name AS common_name,
    regexp_extract(t1.common_name, '.*/([^[/]+)/EQUIPMENT:', 1) AS item_name,
    t1.equipment_name AS equipment_type,
    (t1.installed_from:: DATE) AS installed_from,
    t1.manufacturer AS manufacturer,
    t1.model AS model,
    eav1.attr_value AS specific_model_frame,
    eav2.attr_value AS serial_number,
    t1.asset_status AS asset_status,
    t1.loc_ref AS grid_ref,
    eav3.attr_value AS pandi_tag,
FROM ai2_eav.equipment_masterdata AS t
LEFT JOIN ai2_eav.equipment_eav eav1 ON eav1.ai2_reference = t1.ai2_reference AND eav1.attr_name = 'specific_model_frame'
LEFT JOIN ai2_eav.equipment_eav eav2 ON eav2.ai2_reference = t1.ai2_reference AND eav2.attr_name = 'serial_no'
LEFT JOIN ai2_eav.equipment_eav eav3 ON eav3.ai2_reference = t1.ai2_reference AND eav3.attr_name = 'p_and_i_tag_no'
;


INSERT OR IGNORE INTO ai2_classrep.equi_memo_line BY NAME
SELECT 
    t.ai2_reference AS ai2_reference,
    eav1.attr_value AS memo_line_1,
    eav2.attr_value AS memo_line_2,
    eav3.attr_value AS memo_line_3,
    eav4.attr_value AS memo_line_4,
    eav5.attr_value AS memo_line_5,
FROM ai2_eav.equipment_masterdata AS t
LEFT JOIN ai2_eav.equipment_eav eav1 ON eav1.ai2_reference = t.ai2_reference AND eav1.attr_name = 'memo_line_1'
LEFT JOIN ai2_eav.equipment_eav eav2 ON eav2.ai2_reference = t.ai2_reference AND eav2.attr_name = 'memo_line_2'
LEFT JOIN ai2_eav.equipment_eav eav3 ON eav3.ai2_reference = t.ai2_reference AND eav3.attr_name = 'memo_line_3'
LEFT JOIN ai2_eav.equipment_eav eav4 ON eav4.ai2_reference = t.ai2_reference AND eav4.attr_name = 'memo_line_4'
LEFT JOIN ai2_eav.equipment_eav eav5 ON eav5.ai2_reference = t.ai2_reference AND eav5.attr_name = 'memo_line_5'
;

-- Note - it's currently expected that eav.attr_name is `normalize(attributedescription)`. 
-- This might not be unique and `normalize(attributename)` might be better but this would require
-- more transformation on the source data...
INSERT OR IGNORE INTO ai2_classrep.equi_agasp BY NAME
SELECT 
    t.ai2_reference AS ai2_reference,
    eav1.attr_value AS comments,
    eav2.attr_value AS condition_grade,
    eav3.attr_value AS condition_grade_reason,
    eav4.attr_value AS exclude_from_survey,
    eav5.attr_value AS loading_factor,
    eav6.attr_value AS loading_factor_reason,
    eav7.attr_value AS other_details,
    eav8.attr_value AS performance_grade,
    eav9.attr_value AS performance_grade_reason,
    eav10.attr_value AS reason_for_change,
    eav11.attr_value AS survey_date,
    eav12.attr_value AS survey_year,
    eav13.attr_value AS updated_using,
FROM ai2_eav.equipment_masterdata AS t
LEFT JOIN ai2_eav.equipment_eav eav1 ON eav1.ai2_reference = t.ai2_reference AND eav1.attr_name = 'agasp_comments'
LEFT JOIN ai2_eav.equipment_eav eav2 ON eav2.ai2_reference = t.ai2_reference AND eav2.attr_name = 'condition_grade'
LEFT JOIN ai2_eav.equipment_eav eav3 ON eav3.ai2_reference = t.ai2_reference AND eav3.attr_name = 'condition_grade_reason'
LEFT JOIN ai2_eav.equipment_eav eav4 ON eav4.ai2_reference = t.ai2_reference AND eav4.attr_name = 'set_to_true_for_assets_to_be_excluded_from_survey'
LEFT JOIN ai2_eav.equipment_eav eav5 ON eav5.ai2_reference = t.ai2_reference AND eav5.attr_name = 'loading_factor'
LEFT JOIN ai2_eav.equipment_eav eav6 ON eav6.ai2_reference = t.ai2_reference AND eav6.attr_name = 'loading_factor_reason'
LEFT JOIN ai2_eav.equipment_eav eav7 ON eav7.ai2_reference = t.ai2_reference AND eav7.attr_name = 'agasp_other_details'
LEFT JOIN ai2_eav.equipment_eav eav8 ON eav8.ai2_reference = t.ai2_reference AND eav8.attr_name = 'performance_grade'
LEFT JOIN ai2_eav.equipment_eav eav9 ON eav9.ai2_reference = t.ai2_reference AND eav9.attr_name = 'performance_grade_reason'
LEFT JOIN ai2_eav.equipment_eav eav10 ON eav10.ai2_reference = t.ai2_reference AND eav10.attr_name = 'reason_for_change'
LEFT JOIN ai2_eav.equipment_eav eav11 ON eav11.ai2_reference = t.ai2_reference AND eav11.attr_name = 'agasp_survey_date'
LEFT JOIN ai2_eav.equipment_eav eav12 ON eav12.ai2_reference = t.ai2_reference AND eav12.attr_name = 'agasp_survey_year'
LEFT JOIN ai2_eav.equipment_eav eav13 ON eav13.ai2_reference = t.ai2_reference AND eav13.attr_name = 'updated_using'
;

