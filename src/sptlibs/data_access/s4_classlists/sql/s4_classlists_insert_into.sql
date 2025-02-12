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

CREATE OR REPLACE TABLE s4_classlists.filled_characteristics AS  
WITH cte1 AS 
(SELECT
    first_value(sub.class) OVER (PARTITION BY sub.grp_class ORDER BY sub.row_idx) AS filled_class,
    sub.* EXCLUDE(class),
FROM  (
   SELECT 
        count(t0.class) OVER (ORDER BY t0.row_idx) AS grp_class, 
        t0.*,
   FROM  s4_classlists.dataframe_temp t0
   ) sub
), cte2 AS 
(SELECT
    first_value(sub.characteristic) OVER (PARTITION BY sub.grp_char ORDER BY sub.row_idx) AS filled_characteristic,    
    sub.* EXCLUDE(characteristic),
FROM  (
    SELECT 
        count(t0.characteristic) OVER (ORDER BY t0.row_idx) AS grp_char, 
        t0.*
   FROM  cte1 t0
   ) sub
)
SELECT 
    regexp_extract(t.filled_class, '(\d+) (\w+)', ['class_type', 'class_name']) AS match_grps,
    t.* EXCLUDE(filled_class),
    CASE WHEN match_grps IS NOT NULL THEN match_grps['class_type'] ELSE NULL END AS class_type,
    CASE WHEN match_grps IS NOT NULL THEN match_grps['class_name'] ELSE NULL END AS class_name,
FROM cte2 t
ORDER BY row_idx;

INSERT OR REPLACE INTO s4_classlists.equi_characteristics BY NAME 
WITH cte AS
    (SELECT DISTINCT
        t.class_name AS class_name,
        first_value(t.char_value) OVER (PARTITION BY t.class_name ORDER BY t.row_idx) AS class_description,
    FROM s4_classlists.filled_characteristics t
    WHERE 
        t.data_type IS NULL 
    AND t.class_type == '002'
)
SELECT 
    t.class_name AS class_name,
    t.filled_characteristic AS char_name,
    t1.class_description AS class_description,
    t.char_value AS char_description,
    t.data_type AS char_type,
    t.no_chars AS char_length,
    t.dec_places AS char_precision,
FROM s4_classlists.filled_characteristics t
JOIN cte t1 ON t1.class_name = t.class_name
WHERE 
    t.data_type IS NOT NULL
AND t.value IS NULL    
AND t.class_type == '002'
ORDER BY class_name, char_name;


INSERT OR REPLACE INTO s4_classlists.floc_characteristics BY NAME 
WITH cte AS
    (SELECT DISTINCT
        t.class_name AS class_name,
        first_value(t.char_value) OVER (PARTITION BY t.class_name ORDER BY t.row_idx) AS class_description,
    FROM s4_classlists.filled_characteristics t
    WHERE 
        t.data_type IS NULL 
    AND t.class_type == '003'
)
SELECT 
    t.class_name AS class_name,
    t.filled_characteristic AS char_name,
    t1.class_description AS class_description,
    t.char_value AS char_description,
    t.data_type AS char_type,
    t.no_chars AS char_length,
    t.dec_places AS char_precision,
FROM s4_classlists.filled_characteristics t
JOIN cte t1 ON t1.class_name = t.class_name
WHERE 
    t.data_type IS NOT NULL
AND t.value IS NULL    
AND t.class_type == '003'
ORDER BY class_name, char_name;

INSERT OR REPLACE INTO s4_classlists.equi_enums BY NAME 
SELECT 
    t.class_name AS class_name,
    t.filled_characteristic AS char_name,
    t.value AS enum_value,
    t.char_value AS enum_description,
FROM s4_classlists.filled_characteristics t
WHERE 
    t.data_type IS NULL
AND t.value IS NOT NULL    
AND t.class_type == '002'
ORDER BY class_name, char_name;


INSERT OR REPLACE INTO s4_classlists.floc_enums BY NAME 
SELECT 
    t.class_name AS class_name,
    t.filled_characteristic AS char_name,
    t.value AS enum_value,
    t.char_value AS enum_description,
FROM s4_classlists.filled_characteristics t
WHERE 
    t.data_type IS NULL
AND t.value IS NOT NULL    
AND t.class_type == '003'
ORDER BY class_name, char_name;