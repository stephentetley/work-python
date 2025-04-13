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


WITH cte AS (
    SELECT DISTINCT ON (ai2_equipment_type, s4_class) 
        ai2_equipment_type, 
        s4_class AS class_name, 
        's4_classrep.equiclass_' || lower(s4_class) AS qualified_dest_name,
        udfx.make_snake_case_name(ai2_equipment_type) || '_to_' || udfx.make_snake_case_name(class_name) AS macro_name,
    FROM equi_asset_translation.mapping_worklist
) 
SELECT 
    macro_name AS macro_name,
    concat_ws(E'\n',
        format('INSERT INTO {} BY NAME', qualified_dest_name),
        'SELECT',
        '    t.ai2_reference,', 
        '    t2.* EXCLUDE (equipment_id),',
        'FROM ai2_classrep.equi_masterdata t',
        'JOIN equi_asset_translation.mapping_worklist t1 ON t1.equipment_id = t.ai2_reference',
        format('JOIN {}() t2 ON t2.equipment_id = t1.equipment_id', macro_name),
        format('WHERE t1.ai2_equipment_type = ''{}''', ai2_equipment_type),
        format('AND t1.s4_class = ''{}'';',  class_name)
        ) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;
