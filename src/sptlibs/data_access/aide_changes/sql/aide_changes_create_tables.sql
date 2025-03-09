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

INSTALL excel;
LOAD excel;

CREATE SCHEMA IF NOT EXISTS soev_rawdata;

-- Use raw export names...
CREATE OR REPLACE TABLE soev_rawdata.aide_exports (
    "Date" TIMESTAMP,
    "Asset Ref" VARCHAR,
    "AssetName" VARCHAR,
    "Status" VARCHAR,
);


CREATE OR REPLACE VIEW soev_rawdata.vw_aide_changes AS
WITH cte AS (
SELECT 
    t."Date" As changed_date,
    t."Asset Ref" AS asset_ref,
    t."AssetName" AS _text1,
    regexp_extract(_text1, '(EQUIPMENT:.*)', 1) AS equipment_type,
    regexp_extract(_text1, '([A-Z0-9 ]+)_?', 1) AS _prefix,
    string_split(_text1, _prefix) AS _arr,
    CASE 
        WHEN array_length(_arr) = 3 THEN _prefix || array_extract(_arr, 3)
        ELSE _text1
    END AS common_name, 
    t."Status" AS ai_status,
FROM soev_rawdata.aide_exports t
)
SELECT DISTINCT ON(asset_ref) changed_date, asset_ref, common_name, equipment_type, ai_status FROM cte
ORDER BY common_name
;
