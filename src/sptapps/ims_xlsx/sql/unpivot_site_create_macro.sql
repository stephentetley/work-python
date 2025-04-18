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


CREATE OR REPLACE MACRO unpivot_site(table_name, site_reference) AS TABLE (
WITH cte1 AS (
    SELECT 
        *, 
    FROM query_table(table_name) t
    WHERE t."SiteReference" = site_reference
), cte2 AS (
    FROM cte1 UNPIVOT INCLUDE NULLS (
        attr_value FOR attr_name IN (COLUMNS(*))
    )
)
SELECT  
    attr_name,
    attr_value,
FROM cte2
);


-- Calling examples
-- SELECT * FROM unpivot_site('ims_landing.cso_assets', 'SAI00003607');
-- SELECT * FROM unpivot_site('ims_landing.cso_assets', 'SAI00035751');

