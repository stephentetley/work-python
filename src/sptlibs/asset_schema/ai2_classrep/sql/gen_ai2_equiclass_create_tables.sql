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
        udfx.make_equiclass_name(t.asset_type_description) AS class_name,
        list(struct_pack(field_name := udfx.make_snake_case_name(t.attribute_description), field_type := t.duck_type)) AS field_elements,
    FROM ai2_metadata.vw_specific_equipment_attributes t
    JOIN ai2_classrep.classes_used t1 ON t1.class_name = t.asset_type_description
    WHERE t.asset_type_description LIKE 'EQUIPMENT: %'
    GROUP BY t.asset_type_description
), cte2 AS (
    SELECT 
        lower(t.class_name) AS class_name,
        list_transform(t.field_elements, st -> format(E'    _{} {},', lower(st.field_name), st.field_type)).list_sort().list_distinct() AS field_elements,
    FROM cte1 t
)
SELECT 
    t.class_name AS class_name,
    concat_ws(E'\n',
        format(E'CREATE OR REPLACE TABLE ai2_classrep.{} (', t.class_name),
        '    equipment_id VARCHAR NOT NULL, ',
        list_aggregate(t.field_elements, 'string_agg', E'\n'),
        '    PRIMARY KEY(equipment_id)',
        ');'
        ) AS sql_text,
FROM cte2 t
ORDER BY t.class_name ASC; 
