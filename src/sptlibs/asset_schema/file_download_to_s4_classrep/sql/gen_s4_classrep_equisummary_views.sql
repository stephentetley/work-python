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
        format('CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_{} AS', t.class_name),
        'SELECT', 
        '    emd.equipment_id AS equipment_id,', 
        '    emd.equi_description AS equi_description,',
        '    emd.functional_location AS functional_location,',
        '    emd.manufacturer AS manufacturer,',
        '    emd.model_number AS model_number,',
        '    emd.manufact_part_number AS manufact_part_number,',
        '    emd.serial_number AS serial_number,',
        '    ec.* EXCLUDE (equipment_id),',
        format('FROM s4_classrep.equiclass_{} ec', t.class_name), 
        'JOIN s4_classrep.equi_masterdata emd ON emd.equipment_id = ec.equipment_id;' ) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;