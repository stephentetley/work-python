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

-- AI2 metadata is not sorted out yet as we are relying on an old export.
-- This should go into a specific module in time...

CREATE SCHEMA IF NOT EXISTS ai2_metadata;

CREATE OR REPLACE TABLE ai2_metadata.equipment_attributes (
    asset_type_code VARCHAR,
    asset_type_description VARCHAR, 
    asset_type_deletion_flag BOOLEAN,
    attribute_set VARCHAR, 
    attribute_name_id INTEGER, 
    attribute_description VARCHAR,
    attribute_name VARCHAR, 
    attribute_name_deletion_flag BOOLEAN,
    unit_name VARCHAR, 
    unit_description VARCHAR,
    data_type_name VARCHAR, 
    data_type_description VARCHAR,
);

CREATE OR REPLACE TABLE ai2_metadata.attribute_sets (
    attribute_description VARCHAR, 
    attribute_set VARCHAR,
    class_derivation VARCHAR,
    comment VARCHAR
);

-- For simplicity DATIMES become VARCHAR...
CREATE OR REPLACE VIEW ai2_metadata.vw_live_equipment_attributes AS
WITH cte AS (
    SELECT 
        t.attribute_description AS attribute_description,
        t.attribute_set AS attribute_set
    FROM ai2_metadata.attribute_sets t
    WHERE t.class_derivation = 'Equiclass'
)
SELECT 
    t.* EXCLUDE (attribute_name_deletion_flag, asset_type_deletion_flag),
    t1.attribute_set AS attribute_set_name, 
    CASE 
        WHEN t.data_type_name = 'AIYEAR4'       THEN 'INTEGER'
        WHEN t.data_type_name = 'BOOL'          THEN 'BOOLEAN'
        WHEN t.data_type_name LIKE 'CHAR%'      THEN 'VARCHAR'
        WHEN t.data_type_name LIKE 'DATETIME%'  THEN 'VARCHAR'
        WHEN t.data_type_name LIKE 'DECIMAL%'   THEN 'DECIMAL(28, 6)'
        WHEN t.data_type_name LIKE 'FLOAT53'    THEN 'VARCHAR'
        WHEN t.data_type_name = 'NUMERIC29'     THEN 'VARCHAR'
        WHEN t.data_type_name LIKE 'NUMERIC%'   THEN 'DECIMAL(28, 6)'
        WHEN t.data_type_name LIKE 'INT%'       THEN 'INTEGER'
        WHEN t.data_type_name LIKE 'SMALLINT%'  THEN 'INTEGER'
        WHEN t.data_type_name LIKE 'TINYINT%'   THEN 'INTEGER'
        ELSE 'VARCHAR'
    END AS duck_type, 
FROM ai2_metadata.equipment_attributes t
JOIN cte t1 ON t1.attribute_description = t.asset_type_description AND t1.attribute_set = t.attribute_set 
WHERE t.asset_type_description LIKE 'EQUIPMENT: %'
AND t.attribute_name_deletion_flag = FALSE 
AND t.asset_type_deletion_flag = FALSE
ORDER BY t.asset_type_code ;

CREATE OR REPLACE VIEW ai2_metadata.vw_specific_equipment_attributes AS
SELECT t.* 
FROM ai2_metadata.vw_live_equipment_attributes t
WHERE t.attribute_name NOT LIKE 'AGASP%'
AND t.attribute_name NOT IN ['AssetName', 'AssetReference', 'AssetStatus', 
    'AssetType', 'CommonName', 'InstalledFromDate', 'Manufacturer', 
    'MemoLine1', 'MemoLine2', 'MemoLine3', 'MemoLine4', 'MemoLine5', 
    'ModelName', 'NationalGridReference', 'OperationalResponsibility', 
    'PANDITagNo', 'ReplacementCost', 'SAFETY CRITICAL', 'SAPEquipmentRef',
    'SerialNo', 'SpecificModel/Frame', 'StandbyTeam', 'TransferredToWMS', 
    'WorkCentre']
