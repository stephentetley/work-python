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

CREATE SCHEMA IF NOT EXISTS ai2_export_landing;
CREATE SCHEMA IF NOT EXISTS ai2_export;


CREATE TABLE ai2_export_landing.landing_files(
    qualified_table_name VARCHAR NOT NULL,
    file_name VARCHAR,
    file_path VARCHAR,
);

-- TODO landing tables?


CREATE OR REPLACE TABLE ai2_export.floc_masterdata (
    ai2_reference VARCHAR NOT NULL,
    common_name VARCHAR NOT NULL,
    installed_from DATE,
    hierarchy_key VARCHAR,
    asset_status VARCHAR,
    loc_ref VARCHAR,
    asset_in_aide BOOLEAN,
    PRIMARY KEY(ai2_reference)
);

CREATE OR REPLACE TABLE ai2_export.equi_masterdata (
    ai2_reference VARCHAR NOT NULL,
    common_name VARCHAR NOT NULL,
    installed_from DATE,
    manufacturer VARCHAR,
    model VARCHAR,
    asset_status VARCHAR,
    loc_ref VARCHAR,
    asset_in_aide BOOLEAN,
    PRIMARY KEY(ai2_reference)
);

CREATE OR REPLACE TABLE ai2_export.floc_eavdata(
    ai2_reference VARCHAR NOT NULL,
    attribute_name VARCHAR NOT NULL,
    attribute_value VARCHAR,
    PRIMARY KEY(ai2_reference, attribute_name)
);

CREATE OR REPLACE TABLE ai2_export.equi_eavdata(
    ai2_reference VARCHAR NOT NULL,
    attribute_name VARCHAR NOT NULL,
    attribute_value VARCHAR,
    PRIMARY KEY(ai2_reference, attribute_name)
);

CREATE OR REPLACE MACRO extract_ai2_equi_data_from_raw(table_name) AS TABLE 
SELECT 
    t.reference AS ai2_reference,
    t.common_name AS common_name,
    t.installed_from AS installed_from,
    t.manufacturer AS manufacturer,
    t.model AS model,
    t.assetstatus AS asset_status,
    t.loc_ref AS loc_ref,
    t.asset_in_aide AS asset_in_aide, 
FROM query_table(table_name::VARCHAR) t
WHERE t.common_name LIKE '%/EQUIPMENT:%' OR t.common_name LIKE '%/EQPT:%' 
;

CREATE OR REPLACE MACRO extract_ai2_floc_data_from_raw(table_name) AS TABLE 
SELECT 
    t.reference AS ai2_reference,
    t.common_name AS common_name,
    t.installed_from AS installed_from,
    t.hierarchy_key AS hierarchy_key,
    t.assetstatus AS asset_status,
    t.loc_ref AS loc_ref,
    t.asset_in_aide AS asset_in_aide, 
FROM query_table(table_name::VARCHAR) t
WHERE t.common_name NOT LIKE '%/EQUIPMENT:%' AND t.common_name NOT LIKE '%/EQPT:%' 
;


-- NOTE - pivots use assetid even though it has no use
-- This is because we must pivot on at least one column

CREATE OR REPLACE MACRO extract_ai2_equi_eav_data_from_raw(table_name) AS TABLE 
WITH cte AS (
	SELECT 
		reference AS ai2_reference,
		* EXCLUDE (reference, common_name, installed_from, manufacturer, model, hierarchy_key, assetstatus, loc_ref, asset_in_aide) 
	FROM  query_table(table_name::VARCHAR) 
	WHERE common_name LIKE '%/EQUIPMENT:%' OR common_name LIKE '%/EQPT:%' 
)
UNPIVOT cte
ON COLUMNS (* EXCLUDE (ai2_reference, assetid)), assetid::VARCHAR
INTO 
	NAME attribute_name
	VALUE attribute_value
;


CREATE OR REPLACE MACRO extract_ai2_floc_eav_data_from_raw(table_name) AS TABLE 
WITH cte AS (
	SELECT 
		reference AS ai2_reference,
		* EXCLUDE (reference, common_name, installed_from, manufacturer, model, hierarchy_key, assetstatus, loc_ref, asset_in_aide) 
	FROM  query_table(table_name::VARCHAR) 
	WHERE common_name NOT LIKE '%/EQUIPMENT:%' AND common_name NOT LIKE '%/EQPT:%' 
)
UNPIVOT cte
ON COLUMNS (* EXCLUDE (ai2_reference, assetid)), assetid::VARCHAR
INTO 
	NAME attribute_name
	VALUE attribute_value
;
