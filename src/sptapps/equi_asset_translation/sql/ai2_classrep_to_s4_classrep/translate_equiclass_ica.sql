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
FROM ai2_classrep.equiclass_ultrasonic_level_instrument t
)
SELECT COLUMNS(c -> c NOT LIKE '$_$_%' ESCAPE '$') FROM cte
;
