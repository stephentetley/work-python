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

WITH cte AS (
    SELECT 
        lower(ecl.class_name) AS class_name,
    FROM s4_classlists.vw_equi_class_defs ecl 
    WHERE ecl.is_object_class = true
)
SELECT
    t.class_name AS class_name,
    concat_ws(E'\n',
        format('CREATE OR REPLACE VIEW ai2_class_rep.vw_equisummary_{} AS', t.class_name),
        'SELECT', 
        '    emd.equipment_key AS equipment_key,', 
        '    emd.ai2_reference AS ai2_reference,', 
        '    emd.common_name AS common_name,',
        '    emd.equipment_name AS equipment_name,',
        '    emd.installed_from AS installed_from,',
        '    emd.manufacturer AS manufacturer,',
        '    emd.model AS model,',
        '    emd.specific_model_frame AS specific_model_frame,',
        '    emd.serial_number AS serial_number,',
        '    ec.* EXCLUDE (equipment_key, ai2_reference),',
        format('FROM ai2_class_rep.equiclass_{} ec', t.class_name), 
        'JOIN ai2_class_rep.equi_master_data emd ON emd.equipment_key = ec.equipment_key;' ) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;