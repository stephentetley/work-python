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

CREATE OR REPLACE VIEW ai2_metadata.vw_specific_equipment_attributes AS
SELECT t.* 
FROM ai2_metadata.equipment_attributes t
WHERE t.attributename NOT LIKE 'AGASP%'
AND t.attributename NOT IN ['AssetName', 'AssetReference', 'AssetStatus', 
    'AssetType', 'CommonName', 'InstalledFromDate', 'Manufacturer', 
    'MemoLine1', 'MemoLine2', 'MemoLine3', 'MemoLine4', 'MemoLine5', 
    'ModelName', 'PANDITagNo', 'SAPEquipmentRef',
    'SerialNo', 'SpecificModel/Frame']
;
