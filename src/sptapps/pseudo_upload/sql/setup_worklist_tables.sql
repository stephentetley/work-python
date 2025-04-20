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

CREATE SCHEMA IF NOT EXISTS worklist_extra;


CREATE OR REPLACE TABLE worklist_extra.worklist AS
SELECT 
    t."Reference" AS equipment_id,
    t."Common Name" as ai2_common_name,
    t."S4 Name" as s4_name,
    t."S4 Floc" as s4_floc,
FROM read_xlsx(getvariable('worklist_file')) t;

CREATE OR REPLACE TABLE worklist_extra.checked_location_on_site AS
SELECT 
    t."Reference" AS equipment_id,
    t."New Location On Site" AS location_on_site,
    length(location_on_site) AS location_length,
FROM read_xlsx(getvariable('loc_on_site_file')) t;


