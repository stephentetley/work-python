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

-- # ANALDO
INSERT OR REPLACE INTO s4_classrep.equiclass_analdo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_dissolved_oxygen_instrument t;



-- # CONPNL
INSERT OR REPLACE INTO s4_classrep.equiclass_conpnl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    TRY_CAST(t._current_in AS DECIMAL) AS conp_rated_current_a,
    udf_voltage_ac_or_dc(t._voltage_in_ac_or_dc) AS conp_rated_voltage_units,
    TRY_CAST(t._voltage_in AS INTEGER) AS conp_rated_voltage,
FROM ai2_classrep.equiclass_control_panel t;