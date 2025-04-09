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

SELECT 
    row_number() OVER (ORDER BY t.char_description) AS column_idx,
    t.char_description AS column_heading,
    CASE 
        WHEN t.refined_char_type = 'TEXT' AND t.is_enum = True THEN 'list'
        WHEN t.refined_char_type = 'TEXT' AND t.is_enum = False THEN 'textLength'
        WHEN t.refined_char_type = 'INTEGER' THEN 'whole'
        WHEN t.refined_char_type = 'DECIMAL' THEN 'decimal'
        WHEN t.refined_char_type = 'DATE' THEN 'date'
        ELSE null
    END AS validation_type,
    
    CASE 
        WHEN t.refined_char_type = 'TEXT' AND t.is_enum = True THEN null
        WHEN t.refined_char_type = 'TEXT' AND t.is_enum = False THEN 'lessThanOrEqual'
        WHEN t.refined_char_type = 'INTEGER' THEN 'lessThanOrEqual'
        WHEN t.refined_char_type = 'DECIMAL' THEN 'lessThanOrEqual'
        WHEN t.refined_char_type = 'DATE' THEN 'greaterThanOrEqual'
        ELSE null
    END AS validation_operator,
    
    CASE 
        WHEN t.refined_char_type = 'TEXT' AND t.is_enum = True THEN lower(t.char_name) || '_range'
        WHEN t.refined_char_type = 'TEXT' AND t.is_enum = False THEN try_cast(t.char_len AS VARCHAR)
        WHEN t.refined_char_type = 'INTEGER' THEN repeat('9', t.char_len)
        WHEN t.refined_char_type = 'DECIMAL' THEN repeat('9', t.char_len - (t.char_precision + 1)) || '.' || repeat('9', t.char_precision)
        WHEN t.refined_char_type = 'DATE' THEN '01/01/1900'
        ELSE null
    END AS validation_formula1,
FROM s4_classlists.vw_refined_equi_characteristic_defs t
WHERE t.class_name = getvariable('equiclass_name');

--  Set the `equiclass_name` variable before calling this, e.g.
-- > SET VARIABLE equiclass_name = 'NETWTL';
