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





INSERT OR REPLACE INTO s4_classrep.equiclass_stardo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    udf_format_as_integer_string(t._ip_rating) AS ip_rating,
    t._location_on_site AS location_on_site,
    t._current_in AS star_rated_current_a,
    udf_power_to_killowatts(t._power_units, t._power) AS star_rated_power_kw,
    udf_voltage_ac_or_dc(t._voltage_in_ac_or_dc) AS star_rated_voltage_units,
    t._voltage_in AS star_rated_voltage,
FROM ai2_classrep.equiclass_direct_on_line_starter t;
