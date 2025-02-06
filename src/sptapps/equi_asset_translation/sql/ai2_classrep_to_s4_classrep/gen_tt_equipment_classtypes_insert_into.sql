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



-- NOTE - must be run before translating masterdata

-- Batched into groups on objtype to avoid a huge sql statement... 
WITH cte1 AS (
    SELECT 
        t1.class_name AS classtype,
        classtype[:4] AS objecttype,
        format('equiclass_{}', lower(class_name)) AS table_name, 
    FROM s4_classlists.vw_equi_class_defs t1
    WHERE t1.is_object_class = TRUE 
), cte2 AS (
    SELECT 
        objecttype AS objecttype,
        format('(SELECT equipment_id AS equipment_id, ''{}'' AS objtype, ''{}'' AS classtype FROM s4_classrep.{})', 
            objecttype, classtype, table_name
        ) AS sql_text,
    FROM cte1
    ORDER BY objecttype 
), cte3 AS (
    SELECT 
        objecttype, 
        list(sql_text) AS select_qs,
    FROM cte2
    GROUP BY objecttype
) 
SELECT
    objecttype, 
    concat_ws(E'\n', 
        'INSERT OR REPLACE INTO equi_asset_translation.tt_equipment_classtypes BY NAME (',
        list_aggregate(select_qs, 'string_agg', E'\nUNION BY NAME\n'), 
        ');'
    ) AS sql_text
FROM cte3
ORDER BY objecttype 
;
