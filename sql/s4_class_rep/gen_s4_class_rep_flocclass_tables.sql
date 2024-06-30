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

-- CREATE `flocclass_` tables


WITH cte AS (
    SELECT 
        fch.class_name AS class_name,
        array_agg(format(E'    {} {},', lower(fch.char_name), fch.ddl_data_type)) AS field_elements,
    FROM s4_classlists.vw_refined_floc_characteristic_defs fch
    JOIN s4_classlists.vw_floc_class_defs fcl ON fcl.class_name = fch.class_name 
    WHERE fcl.is_system_class = true
    GROUP BY fch.class_name
)
SELECT 
    t.class_name AS class_name,
    concat_ws(E'\n',
        format(E'CREATE OR REPLACE TABLE s4_class_rep.flocclass_{} (', lower(t.class_name)),
        '    floc_id VARCHAR NOT NULL, ',
        list_sort(t.field_elements).list_aggregate('string_agg', E'\n'),
        '    PRIMARY KEY(floc_id)',
        ');'
        ) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;