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

CREATE SCHEMA IF NOT EXISTS rts_outstations;

CREATE OR REPLACE TEMPORARY MACRO rewrite_os_addr(os_address) AS
    regexp_replace(os_address :: VARCHAR, ',\s*', '_');


CREATE OR REPLACE TEMPORARY MACRO extract_ip_addr(ip_address) AS
    regexp_extract(ip_address :: VARCHAR, '(\d*\.\d*.\d*.\d*)', 0);

CREATE OR REPLACE TEMPORARY MACRO extract_datetime(timestamp_str) AS
    try(strptime(timestamp_str, '%H:%M %d-%b-%y'));



CREATE OR REPLACE TABLE rts_outstations.outstations (
    os_name VARCHAR NOT NULL,
    od_name VARCHAR NOT NULL,
    od_comment VARCHAR,
    os_comment VARCHAR,
    scan_status VARCHAR,
    last_polled DATETIME,
    last_power_up DATETIME,
    set_name VARCHAR,
    media_type VARCHAR,
    ip_address VARCHAR,
    os_address VARCHAR,
    os_type VARCHAR,
    PRIMARY KEY(os_name)
);

CREATE OR REPLACE TEMPORARY TABLE outstations_landing (
    "OS name" VARCHAR NOT NULL,
    "OD name" VARCHAR NOT NULL,
    "OD comment" VARCHAR,
    "OS comment" VARCHAR,
    "Scan" VARCHAR,
    "Last polled" VARCHAR,
    "Last power up" VARCHAR,
    "Set name" VARCHAR,
    "Media" VARCHAR,
    "IP Address,  1'ary IP Route,2'ary IP Route" VARCHAR,
    "OS Addr" VARCHAR,
    "OS type" VARCHAR
);

CREATE OR REPLACE MACRO read_outstations_report(tsv_file) AS TABLE
WITH trimmed1 AS (
    SELECT 
        TRIM(COLUMNS(*))
    FROM read_csv(tsv_file :: VARCHAR, delim='\t')
), expanded2 AS (
    (SELECT * FROM outstations_landing LIMIT 0)
    UNION ALL BY NAME
    SELECT * FROM trimmed1
), renamed3 AS (
    SELECT 
        "OS name" AS os_name, 
        "OD name" AS od_name, 
        "OD comment" AS od_comment,
        "OS comment" AS os_comment,
        "Scan" AS scan_status,
        extract_datetime("Last polled") AS last_polled,
        extract_datetime("Last power up") AS last_power_up,
        "Set name" AS set_name,
        "Media" AS media_type,
        extract_ip_addr("IP Address,  1'ary IP Route,2'ary IP Route") AS ip_address,
        rewrite_os_addr("OS Addr") AS os_address,
        "OS type" AS os_type,
    FROM expanded2 
)
SELECT * FROM renamed3;



PREPARE load_outstations AS 
    INSERT INTO rts_outstations.outstations BY NAME
    FROM read_outstations_report($1);

-- To use at a SQL prompt:

-- EXECUTE load_outstations('/home/stephen/_working/work/2025/rts/rts_outstations_report_20250625.tsv');


