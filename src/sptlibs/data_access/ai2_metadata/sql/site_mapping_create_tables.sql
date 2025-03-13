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

CREATE SCHEMA IF NOT EXISTS ai2_metadata;

CREATE OR REPLACE TABLE ai2_metadata.site_mapping (
   site_name VARCHAR NOT NULL,
   installation_name VARCHAR NOT NULL,
   site_sai_num VARCHAR,
   inst_sai_num VARCHAR,
   installation_type VARCHAR,
   user_status VARCHAR, 
   s4_site_code VARCHAR,
   s4_site_name VARCHAR,
   postcode VARCHAR,
);