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

-- INSERT INTO `equiclass_` tables



WITH cte AS (
    SELECT 
        ech.class_name AS class_name,
        array_agg(format(E'    any_value(CASE WHEN eav.charid = ''{}'' THEN {} ELSE NULL END) AS {},', 
                    ech.char_name,
                    CASE 
                        WHEN ech.refined_char_type = 'DECIMAL' THEN 'TRY_CAST(eav.atflv AS DECIMAL)' 
                        WHEN ech.refined_char_type = 'INTEGER' THEN 'round(eav.atflv, 0)' 
                        ELSE 'eav.atwrt'
                    END,                    
                    lower(ech.char_name))) AS field_elements,
    FROM s4_classlists.vw_refined_equi_characteristic_defs ech
    JOIN s4_classlists.vw_equi_class_defs ecl ON ecl.class_name = ech.class_name 
    WHERE ecl.is_object_class = true
    GROUP BY ech.class_name
)
SELECT 
    t.class_name AS class_name,
    concat_ws(E'\n',
        format('INSERT OR REPLACE INTO s4_class_rep.equiclass_{} BY NAME', lower(t.class_name)),
        'SELECT DISTINCT ON(e.equipment_id)', 
        '    e.equipment_id AS equipment_id,',
        list_sort(t.field_elements).list_aggregate('string_agg', E'\n'),
        'FROM s4_class_rep.equi_master_data e',
        'JOIN s4_fd_raw_data.classequi_classequi1 clz ON clz.equi = e.equipment_id',
        'JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id',
        format('WHERE clz.class = ''{}''', t.class_name),
        'GROUP BY e.equipment_id;') AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;