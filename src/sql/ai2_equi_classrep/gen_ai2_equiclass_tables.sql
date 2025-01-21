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

-- ## Create AI2 `equiclass_` tables



WITH cte1 AS (
    SELECT 
        t1.assettypecode AS class_name,
        list(struct_pack(field_name := ai2_classrep.normalize_name(t1.attributedescription), field_type := 'VARCHAR')) AS field_elements,
    FROM ai2_metadata.equipment_attributes t1
    GROUP BY t1.assettypecode
), cte2 AS (
    SELECT 
        lower(t1.class_name) AS class_name,
        list_transform(t1.field_elements, st -> format(E'    {} {},', lower(st.field_name), st.field_type)).list_sort() AS field_elements,
    FROM cte1 t1
)
SELECT 
    t.class_name AS class_name,
    concat_ws(E'\n',
        format(E'CREATE OR REPLACE TABLE ai2_classrep.equiclass_{} (', t.class_name),
        '    equipment_id VARCHAR NOT NULL, ',
        list_aggregate(t.field_elements, 'string_agg', E'\n'),
        '    PRIMARY KEY(equipment_id)',
        ');'
        ) AS sql_text,
FROM cte2 t
ORDER BY t.class_name ASC; 
