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
    SELECT 
        ech.class_name AS class_name,
        array_agg(format(E'    {} AS {},', make_tostring_expression(ech.ddl_data_type, ech.char_precision, ech.char_name), ech.char_name )) AS field_elements,
    FROM s4_classlists.vw_refined_equi_characteristic_defs ech
    JOIN s4_classlists.vw_equi_class_defs ecl ON ecl.class_name = ech.class_name 
    WHERE ecl.is_object_class = true
    GROUP BY ech.class_name
)
SELECT 
    t.class_name AS class_name,
    concat_ws(E'\n',
        'INSERT INTO s4_uploader.eq_classification BY NAME',
        'WITH cte AS (',
        format('UNPIVOT s4_classrep.equiclass_{}', lower(class_name)),
        'ON',
        list_sort(t.field_elements).list_aggregate('string_agg', E'\n'),
        'INTO',
        '    NAME characteristics',
        '    VALUE char_value)',
        'SELECT',
        format('    *, ''{}'' AS class, ', upper(class_name)),
        'FROM cte;'
        ) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;
