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


-- Returns a table that can be written directly to Excel for picklist ranges
-- Can't currently easily turn this into a macro as it use PIVOT

WITH cte1 AS (
    SELECT 
        t.* EXCLUDE (enum_description),
    FROM s4_classlists.equi_enums t
    WHERE t.class_name = getvariable('table_name')
    ORDER BY t.enum_value
), cte2 AS (
    PIVOT cte1
    ON char_name
    USING list(enum_value)
)
SELECT unnest(COLUMNS(* EXCLUDE (class_name))) from cte2
;

--  Set the table_name variable before calling this, e.g.
-- > SET VARIABLE table_name = 'NETWTL';
