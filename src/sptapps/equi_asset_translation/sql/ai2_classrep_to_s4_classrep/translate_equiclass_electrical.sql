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

INSERT OR REPLACE INTO s4_classrep.equiclass_emtrin BY NAME
WITH cte AS (
SELECT
    t.equipment_id AS equipment_id,   
    t._insulation_class AS insulation_class_deg_c, 
    t._ip_rating AS ip_rating,
    t._location_on_site AS location_on_site,
    -- emtr_rated_current
    TRY_CAST(t._current_in AS DECIMAL) AS emtr_rated_current_a,
    -- emtr_rated_power_kw
    TRY_CAST(t._power AS DECIMAL) AS __dec_power,
    equi_asset_translation.power_to_killowatts(t._power_units, __dec_power) AS emtr_rated_power_kw,
    -- emtr_rated_voltage / emtr_rated_voltage_units
    equi_asset_translation.voltage_ac_or_dc(t._voltage_in_ac_or_dc) AS emtr_rated_voltage_units,
    TRY_CAST(t._voltage_in AS INTEGER) AS emtr_rated_voltage,
FROM ai2_classrep.equiclass_non_immersible_motor t
)
SELECT COLUMNS(c -> c NOT LIKE '$_$_%' ESCAPE '$') FROM cte
;


INSERT OR REPLACE INTO s4_classrep.equiclass_stardo BY NAME
WITH cte AS (
SELECT
    t.equipment_id AS equipment_id,  
    t._ip_rating AS ip_rating,
    t._location_on_site AS location_on_site,
    -- star_rated_current
    TRY_CAST(t._current_in AS DECIMAL) AS star_rated_current_a,
    -- star_rated_power_kw
    TRY_CAST(t._power AS DECIMAL) AS __dec_power,
    equi_asset_translation.power_to_killowatts(t._power_units, __dec_power) AS star_rated_power_kw,
    -- star_rated_voltage / star_rated_voltage_units
    equi_asset_translation.voltage_ac_or_dc(t._voltage_in_ac_or_dc) AS star_rated_voltage_units,
    TRY_CAST(t._voltage_in AS INTEGER) AS star_rated_voltage,
FROM ai2_classrep.equiclass_direct_on_line_starter t
)
SELECT COLUMNS(c -> c NOT LIKE '$_$_%' ESCAPE '$') FROM cte
;
