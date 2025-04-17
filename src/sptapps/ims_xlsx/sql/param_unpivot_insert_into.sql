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

-- Use standard SQL UNPIVOT syntax to preserve nulls ...
-- DuckDb syntax does allow `INCLUDE NULLS`


-- Run this with params `file_path` & `tab_name`
INSERT INTO ims_reports.source_unpivoted
WITH cte1 AS (
    SELECT 
        row_number() OVER () AS group_idx, 
        *, 
    FROM read_xlsx(:file_path, sheet=:tab_name, all_varchar=true)
), cte2 AS (
    FROM cte1 UNPIVOT INCLUDE NULLS (
        attr_value FOR attr_name IN (COLUMNS(* EXCLUDE(group_idx)))
    )
)
SELECT 
    :tab_name AS source_type,
    group_idx, 
    row_number() OVER (PARTITION BY group_idx) AS element_idx, 
    attr_name,
    attr_value,
FROM cte2
ORDER BY source_type, group_idx, element_idx
;

