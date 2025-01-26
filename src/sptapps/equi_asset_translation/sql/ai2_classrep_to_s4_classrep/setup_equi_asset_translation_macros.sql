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


CREATE OR REPLACE MACRO equi_asset_translation.voltage_ac_or_dc(ac_or_dc) AS (
    CASE 
        WHEN upper(ac_or_dc) = 'DIRECT CURRENT' THEN 'VDC' 
        WHEN upper(ac_or_dc) = 'ALTERNATING CURRENT' THEN 'VAC'
        ELSE NULL
    END
);


CREATE OR REPLACE MACRO equi_asset_translation.size_to_millimetres(size_units, size_value) AS (
    CASE 
        WHEN upper(size_units) = 'MILLIMETRES' THEN round(size_value, 0)
        WHEN upper(size_units) = 'CENTIMETRES' THEN round(size_value  * 10, 0) 
        WHEN upper(size_units) = 'METRES' THEN round(size_value  * 1000, 0) 
        WHEN upper(size_units) = 'INCH' THEN round(size_value * 25.4, 0) 
        ELSE NULL
    END
);

CREATE OR REPLACE MACRO equi_asset_translation.power_to_killowatts(power_units, power_value) AS (
    CASE 
        WHEN upper(power_units) = 'KILOWATTS' THEN power_value
        WHEN upper(power_units) = 'WATTS' THEN power_value  * 1000.0  
        ELSE NULL
    END
);