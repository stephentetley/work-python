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

CREATE SCHEMA IF NOT EXISTS ims_reports;


CREATE OR REPLACE TABLE ims_reports.source_unpivoted (
    source_type VARCHAR NOT NULL,
    group_idx INTEGER NOT NULL,
    element_idx INTEGER NOT NULL,
    attr_name VARCHAR NOT NULL,
    attr_value VARCHAR,
);