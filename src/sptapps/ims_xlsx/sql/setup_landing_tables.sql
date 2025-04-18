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

CREATE SCHEMA IF NOT EXISTS ims_landing;

-- Variable seems to be the best (only?) choice for parameterizing CREATE TABLE ... AS SELECT ...  

-- Set the variable `file_source` before running this script
-- SET VARIABLE file_source = 'g:/work/2025/sps_for_ims/source_files/AI2MitigationPlans20250212.xlsx';

CREATE OR REPLACE TABLE ims_landing.sps_assets AS
SELECT *, 
FROM read_xlsx(getvariable('file_source'), sheet='SPS', all_varchar=true);

CREATE OR REPLACE TABLE ims_landing.dtk_assets AS
SELECT *, 
FROM read_xlsx(getvariable('file_source'), sheet='DTK', all_varchar=true);

CREATE OR REPLACE TABLE ims_landing.inlet_pumping_assets AS
SELECT *, 
FROM read_xlsx(getvariable('file_source'), sheet='InletPumping', all_varchar=true);

CREATE OR REPLACE TABLE ims_landing.cso_assets AS
SELECT *, 
FROM read_xlsx(getvariable('file_source'), sheet='CSO', all_varchar=true);

CREATE OR REPLACE TABLE ims_landing.cso_storage_assets AS
SELECT *, 
FROM read_xlsx(getvariable('file_source'), sheet='CSO Storage', all_varchar=true);

CREATE OR REPLACE TABLE ims_landing.suds_assets AS
SELECT *, 
FROM read_xlsx(getvariable('file_source'), sheet='SuDS', all_varchar=true);
