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


USE ai2_landing;


WITH cte AS (
SELECT 
    list(t1.table_name) AS table_names,
FROM 
    duckdb_tables() t1,
WHERE t1.schema_name = 'ai2_landing'
)
SELECT 
    list_transform(cte.table_names, s -> format('SELECT * FROM get_equipment_attr_values(''{}'')', s)) AS table_qs,
    concat_ws(E'\n', 
        'INSERT INTO ai2_eav.equipment_eav BY NAME (',
        list_aggregate(table_qs, 'string_agg', E'\nUNION BY NAME\n'), 
        ');'
    ) AS sql_text
FROM cte;
