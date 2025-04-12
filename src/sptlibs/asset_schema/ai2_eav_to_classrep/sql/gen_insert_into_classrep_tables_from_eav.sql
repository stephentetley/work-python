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

-- ## Create INSERT INTO `equiclass_` tables from AI2 eav




WITH cte1 AS (
    SELECT 
        t.asset_type_description AS name,
        list(t.attribute_description) AS attributes,
    FROM ai2_metadata.vw_specific_equipment_attributes t
    WHERE t.asset_type_description LIKE 'EQUIPMENT: %'
    GROUP BY t.asset_type_description
), 
cte2 AS (
    -- TODO select_fields is expected to need casting...
    SELECT
        udfx.make_equiclass_name(t.name) AS class_name,
        t.name AS equipment_name,
        list_transform(t.attributes, s -> udfx.make_snake_case_name(s)).list_distinct() AS normed_attributes,
        list_zip(range(1, 100), normed_attributes, true) AS numbered_attributes,
        list_transform(numbered_attributes, pair -> format('    eav{}.attr_value AS _{},', pair[1], pair[2])) AS select_fields,
        list_transform(numbered_attributes, pair -> format('    LEFT JOIN ai2_eav.equipment_eav eav{} ON t.ai2_reference = eav{}.ai2_reference AND eav{}.attr_name = ''{}''', pair[1], pair[1], pair[1], pair[2])) AS left_joins,
    FROM cte1 t
)
SELECT 
    t.class_name AS class_name,
    concat_ws(E'\n',
        format(E'INSERT OR REPLACE INTO ai2_classrep.{} BY NAME', t.class_name),
        'SELECT',  
        '    t.ai2_reference AS equipment_id,', 
        list_aggregate(t.select_fields, 'string_agg', E'\n'),
        'FROM',  
        '    ai2_eav.equipment_masterdata t', 
        list_aggregate(t.left_joins, 'string_agg', E'\n'),
        'WHERE',
        format('    t.equipment_name = ''{}'';', equipment_name)
        ) AS sql_text,
FROM cte2 t
ORDER BY t.class_name ASC; 