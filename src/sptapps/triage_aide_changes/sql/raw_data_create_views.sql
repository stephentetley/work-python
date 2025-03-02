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



CREATE OR REPLACE VIEW raw_data.vw_ai2_site_export_equi_names AS
WITH cte1 AS (
SELECT 
    t.reference, 
    t.common_name,
    regexp_extract(t.common_name, '(.*)/(EQUIPMENT:.*)', ['prefix', 'equitype']) AS name_struct,
    struct_extract(name_struct, 'prefix') AS prefix,
    struct_extract(name_struct, 'equitype') AS equi_type,
    length(prefix) AS prefix_len,
FROM raw_data.ai2_site_export t
WHERE t.common_name LIKE '%/EQUIPMENT:%'
), cte2 AS (
SELECT 
    t.reference, 
    t.common_name,
    t.prefix,
    t.equi_type,
    t1.common_name AS candidate,
FROM cte1 t
JOIN raw_data.ai2_site_export t1 ON starts_with(t.common_name, t1.common_name) AND length(t1.common_name) < t.prefix_len
), cte3 AS (
SELECT     
    t.reference, 
    t.common_name,
    max(t.candidate) AS longest_prefix,
    length(longest_prefix) AS pos,
    t.prefix[pos+2:] AS equi_name,
    t.equi_type AS equi_type,
FROM cte2 t
GROUP BY t.reference, t.common_name, t.prefix, t.equi_type
)
SELECT t.reference, t.common_name, t.equi_name, t.equi_type FROM cte3 t
ORDER BY common_name
;

CREATE OR REPLACE VIEW raw_data.vw_ai2_parent_sai_nums AS
WITH cte1 AS (
SELECT 
    t.reference AS pli_num, 
    t.common_name,
    regexp_extract(t.common_name, '(.*)/(EQUIPMENT:.*)', ['prefix', 'equitype']) AS name_struct,
    struct_extract(name_struct, 'prefix') AS prefix,
    struct_extract(name_struct, 'equitype') AS equi_type,
    length(prefix) AS prefix_len,
FROM raw_data.ai2_site_export t
WHERE t.common_name LIKE '%/EQUIPMENT:%'
), cte2 AS (
SELECT 
    t.pli_num AS pli_num, 
    t1.reference AS sai_num,
    t.common_name AS common_name,
FROM cte1 t
JOIN raw_data.ai2_site_export t1 ON t1.common_name = t.prefix
) 
SELECT t.* FROM cte2 t
ORDER BY common_name
;

