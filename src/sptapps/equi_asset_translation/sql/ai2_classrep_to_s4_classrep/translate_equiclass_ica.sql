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

-- LSTNCO
INSERT OR REPLACE INTO s4_classrep.equiclass_lstnco BY NAME
WITH cte AS (
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    equi_asset_translation.format_output_type(t._signal_unit) AS lstn_output_type,
    t._range_min AS lstn_range_min,
    t._range_max AS lstn_range_max,
    upper(t._range_unit) AS lstn_range_units,
    equi_asset_translation.format_signal(t._signal_min, t._signal_max, t._signal_unit) AS lstn_signal_type,
FROM ai2_classrep.equiclass_conductivity_level_instrument t
)
SELECT COLUMNS(c -> c NOT LIKE '$_$_%' ESCAPE '$') FROM cte
;


-- LSTNUT
INSERT OR REPLACE INTO s4_classrep.equiclass_lstnut BY NAME
WITH cte AS (
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    -- relay 1
    t._relay_1_function AS lstn_relay_1_function,
    TRY_CAST(t._relay_1_off_level_m AS DECIMAL) AS lstn_relay_1_off_level_m,
    TRY_CAST(t._relay_1_on_level_m AS DECIMAL) AS lstn_relay_1_on_level_m,
    -- relay 2
    t._relay_2_function AS lstn_relay_2_function,
    TRY_CAST(t._relay_2_off_level_m AS DECIMAL) AS lstn_relay_2_off_level_m,
    TRY_CAST(t._relay_2_on_level_m AS DECIMAL) AS lstn_relay_2_on_level_m,
    -- relay 3
    t._relay_3_function AS lstn_relay_3_function,
    TRY_CAST(t._relay_3_off_level_m AS DECIMAL) AS lstn_relay_3_off_level_m,
    TRY_CAST(t._relay_3_on_level_m AS DECIMAL) AS lstn_relay_3_on_level_m,
    -- relay 4
    t._relay_4_function AS lstn_relay_4_function,
    TRY_CAST(t._relay_4_off_level_m AS DECIMAL) AS lstn_relay_4_off_level_m,
    TRY_CAST(t._relay_4_on_level_m AS DECIMAL) AS lstn_relay_4_on_level_m,
    -- relay 5
    t._relay_5_function AS lstn_relay_5_function,
    TRY_CAST(t._relay_5_off_level_m AS DECIMAL) AS lstn_relay_5_off_level_m,
    TRY_CAST(t._relay_5_on_level_m AS DECIMAL) AS lstn_relay_5_on_level_m,
    -- relay 6
    t._relay_6_function AS lstn_relay_6_function,
    TRY_CAST(t._relay_6_off_level_m AS DECIMAL) AS lstn_relay_6_off_level_m,
    TRY_CAST(t._relay_6_on_level_m AS DECIMAL) AS lstn_relay_6_on_level_m,
    
    t._transducer_type AS lstn_transducer_model,
    t._transducer_serial_no AS lstn_transducer_serial_no,
    t._range_min AS lstn_range_min,
    t._range_max AS lstn_range_max,
    upper(t._range_unit) AS lstn_range_units,
    equi_asset_translation.format_signal(t._signal_min, t._signal_max, t._signal_unit) AS lstn_signal_type,
FROM ai2_classrep.equiclass_ultrasonic_level_instrument t
)
SELECT COLUMNS(c -> c NOT LIKE '$_$_%' ESCAPE '$') FROM cte
;

-- NETWTL
INSERT OR REPLACE INTO s4_classrep.equiclass_netwtl BY NAME
WITH cte AS (
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_telemetry_outstation t
)
SELECT COLUMNS(c -> c NOT LIKE '$_$_%' ESCAPE '$') FROM cte
;
