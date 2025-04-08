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


-- Returns "dimensions" of enums for turning into Excel ranges

WITH cte AS (
    SELECT 
        t.char_name, 
        count(t.enum_value) as enum_count,
    FROM s4_classlists.equi_enums t
    WHERE t.class_name = getvariable('equiclass_name')
    GROUP BY t.char_name
)
SELECT lower(char_name) || '_range' AS range_name, row_number() OVER (ORDER BY char_name) AS column_idx, enum_count FROM cte;

--  Set the `equiclass_name` variable before calling this, e.g.
-- > SET VARIABLE equiclass_name = 'NETWTL';
