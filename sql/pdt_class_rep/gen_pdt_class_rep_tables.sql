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

-- CREATE `equiclass_` tables


WITH cte AS (
    SELECT 
        ech.class_name AS class_name,
        array_agg(format(E'    {} {},', lower(ech.char_name), ech.ddl_data_type)) AS field_elements,
    FROM s4_classlists.vw_refined_equi_characteristic_defs ech
    JOIN s4_classlists.vw_equi_class_defs ecl ON ecl.class_name = ech.class_name 
    WHERE ecl.is_object_class = true
    GROUP BY ech.class_name
)
SELECT 
    t.class_name AS class_name,
    format(E'CREATE OR REPLACE TABLE pdt_class_rep.equiclass_{} (\n    {}\n    {}\n);', 
        lower(t.class_name),
        'equipment_key UBIGINT,',
        list_sort(t.field_elements).list_aggregate('string_agg', E'\n')
        ) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC; 