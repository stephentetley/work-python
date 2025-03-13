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

--- depends on ai2_metadata.process_group_names and ai2_metadata.process_names

-- "Common Name" column is now parametric!
CREATE OR REPLACE MACRO split_common_name(table_name, col_name) AS TABLE 
WITH cte1 AS (
    SELECT 
        COLUMNS(col_name::VARCHAR) AS common_name,
    FROM query_table(table_name::VARCHAR)
), cte2 AS (
    SELECT 
        t.common_name AS common_name,
        '/' || t1.process_group_name || '/' AS process_group,
        '/' || t2.process_name || '/' AS process,
    FROM cte1 t
    LEFT JOIN ai2_metadata.process_group_names t1 ON contains(t.common_name, '/' || t1.process_group_name || '/')
    JOIN ai2_metadata.process_names t2 ON contains(t.common_name, '/' || t2.process_name || '/')
), cte3 AS (
SELECT 
    t.common_name AS common_name,
    t.process_group AS process_group,
    t.process AS process,
    position(process_group IN common_name) AS processs_group_start,
    position(process IN common_name) AS processs_start,
    position('/EQUIPMENT:' IN common_name) AS equipment_start,
    CASE 
        WHEN processs_group_start > 0 THEN processs_group_start-1
        WHEN processs_start > 0 THEN processs_start-1
        ELSE 0
    END AS site_name_end,
    common_name[1:site_name_end] AS site,
    CASE 
        WHEN processs_start > 0 THEN processs_start + length(process)
        ELSE 0
    END AS item_name_start,
    CASE
        WHEN processs_start > 0 AND equipment_start > 0 THEN common_name[item_name_start:equipment_start-1]
        WHEN processs_start > 0 THEN common_name[item_name_start:] 
        ELSE ''
    END AS item_name,
    CASE
        WHEN equipment_start > 0 THEN common_name[equipment_start+12:]
        ELSE '' 
    END AS equipment_type,
    FROM cte2 t
)
SELECT common_name, min(site) AS site, item_name, equipment_type
FROM cte3
WHERE site <> '' AND item_name <> ''
GROUP BY common_name, item_name, equipment_type
;
