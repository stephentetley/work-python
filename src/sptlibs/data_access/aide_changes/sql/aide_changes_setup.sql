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

CREATE SCHEMA IF NOT EXISTS aide_changes;
CREATE SCHEMA IF NOT EXISTS aide_changes_landing;

CREATE TABLE aide_changes_landing.landing_files(
    table_name VARCHAR,
    file_name VARCHAR
);

CREATE OR REPLACE TABLE aide_changes.change_export_all (
    source_file VARCHAR NOT NULL,
    asset_ref VARCHAR NOT NULL,
    change_date DATETIME,
    common_name VARCHAR,
    request_status VARCHAR,
    request_type VARCHAR,
);

CREATE OR REPLACE VIEW aide_changes_landing.vw_landing_tables AS
WITH cte AS (
    SELECT 
        t.table_name, 
        t.table_schema, 
        t.table_schema || '.' || t.table_name AS qualified_table_name,
    FROM information_schema.tables t
    WHERE table_schema = 'aide_changes_landing'
    AND table_name SIMILAR TO 'export(\d+)'
)
SELECT 
    cte.*,
    t1.file_name AS file_name
FROM cte 
LEFT JOIN aide_changes_landing.landing_files t1 ON t1.table_name = cte.qualified_table_name;


CREATE OR REPLACE MACRO get_glob_matches(glob_pattern) AS TABLE (
    SELECT 
        row_number() OVER () AS file_index, 
        t.file AS file_path,
        parse_filename(file_path, 'false', 'system') AS file_name,
    FROM glob(glob_pattern::VARCHAR) t
);

CREATE OR REPLACE MACRO get_changes_from_export(table_name) AS TABLE (
SELECT 
    t.source_file AS source_file,
    t."Asset Ref" AS asset_ref,
    t."Date" AS change_date,
    t."AssetName" AS common_name,
    t."Status" AS request_status,
    t."Request Type" AS request_type,
FROM query_table(table_name::VARCHAR) t
);
