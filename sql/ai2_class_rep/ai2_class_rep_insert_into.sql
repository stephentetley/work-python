-- 
-- Copyright 2024 Stephen Tetley
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



--- source table may have _duplicates_ hence use `DISTINCT ON`...
INSERT OR REPLACE INTO ai2_class_rep.asset_condition BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'condition_grade' THEN eav.attribute_value ELSE NULL END) AS condition_grade,
    any_value(CASE WHEN eav.attribute_name = 'condition_grade_reason' THEN eav.attribute_value ELSE NULL END) AS condition_grade_reason,
    any_value(CASE WHEN eav.attribute_name = 'agasp_survey_year' THEN TRY_CAST(eav.attribute_value AS INTEGER) ELSE NULL END) AS survey_date,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT:%'
GROUP BY emd.ai2_reference;