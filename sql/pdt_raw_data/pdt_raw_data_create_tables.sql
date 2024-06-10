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


CREATE SCHEMA IF NOT EXISTS pdt_raw_data;

CREATE OR REPLACE TABLE pdt_raw_data.pdt_file(
    base_name VARCHAR NOT NULL,
    pdt_type VARCHAR,
    system_floc VARCHAR, 
    system_name VARCHAR,
);

CREATE OR REPLACE TABLE pdt_raw_data.pdt_eav(
    base_name VARCHAR NOT NULL,
    entity_name VARCHAR NOT NULL,
    attr_name VARCHAR, 
    attr_value VARCHAR,
);

