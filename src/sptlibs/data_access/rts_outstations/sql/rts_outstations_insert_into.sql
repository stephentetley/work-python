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

INSERT INTO rts_outstations.outstations BY NAME
WITH cte1 AS (
-- Fill out fields of interest with `null` if missing from the import...
SELECT * FROM rts_import_file_schema
UNION ALL BY NAME
SELECT * FROM read_csv($csv_file_path)
), cte2 AS (
SELECT 
    trim(t."OS name") AS os_name,
    trim(t."OD name") AS od_name,
    trim(t."OD comment") AS od_comment,
    trim(t."OS comment") AS os_comment,
    trim(t."Scan") AS scan_status,
    try_strptime("Last polled", '%H:%M %d-%b-%y') AS last_polled,
    try_strptime("Last power up", '%H:%M %d-%b-%y') AS last_power_up,
    trim(t."Set name") AS set_name,
    trim(t."Media") AS media_type,
    trim(t."IP Address,  1'ary IP Route,2'ary IP Route") AS ip_address,
    trim(t."OS Addr") AS os_address,
    trim(t."OS type") AS os_type,
FROM cte1 t
)
SELECT * FROM cte2
;