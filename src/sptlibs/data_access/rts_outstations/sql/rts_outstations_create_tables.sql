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

CREATE OR REPLACE TEMPORARY TABLE rts_import_file_schema (
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
    "OS type" VARCHAR,
);

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
