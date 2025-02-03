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

-- # DAMSCO Dam Core (no ai2 match)

-- # DAMSCR Dam Crest
INSERT OR REPLACE INTO s4_classrep.equiclass_damscr BY NAME
SELECT
    t.equipment_id AS equipment_id,
FROM ai2_classrep.equiclass_dam_crest t;

-- # DAMSCT Cut off Trench (no ai2 match)

-- # DAMSDM Dam
INSERT OR REPLACE INTO s4_classrep.equiclass_damsdm BY NAME
SELECT
    t.equipment_id AS equipment_id,
FROM ai2_classrep.equiclass_dam t;

-- # DAMSFD Dam Face Downstream (no ai2 match)

-- # DAMSFP Dam Face Upstream (no ai2 match)

-- # DAMSSB Stilling Basin
INSERT OR REPLACE INTO s4_classrep.equiclass_damssb BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_stilling_basin t;

-- # DAMSTB Tumble Bay
INSERT OR REPLACE INTO s4_classrep.equiclass_damstb BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_tumble_bay t;

-- # DAMSTU Tunnel
INSERT OR REPLACE INTO s4_classrep.equiclass_damstu BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_tunnel t;

-- # DAMSWW Wave Wall
INSERT OR REPLACE INTO s4_classrep.equiclass_damsww BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_wave_wall t;

-- # DECOEB Emergency Eye Wash Station
INSERT OR REPLACE INTO s4_classrep.equiclass_decoeb BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_emergency_eye_bath t;

-- # DECOES Emergency Eye Wash and Shower
INSERT OR REPLACE INTO s4_classrep.equiclass_decoes BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_emergency_shower_eye_bath t;

-- # DECOSH Emergency Shower
INSERT OR REPLACE INTO s4_classrep.equiclass_decosh BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_emergency_shower t;

-- # DECTEX Gas Detector for Explosive Atmospheres (no ai2 match)
-- # DECTFM Detector for boiler flame sources (no ai2 match)
-- # DECTPA Monitor Panel for Gas Detectors (no ai2 match)
-- # DECTTX Gas Detector for Toxic Atmospheres (no ai2 match)
-- # DESTOZ Ozone Destructor (no ai2 match)
-- # DHUMAC Air Conditioning Dehumidifier (no ai2 match)
-- # DHUMDE Desiccant Dehumidifier (no ai2 match)

-- # DISTBD Distribution Board
INSERT OR REPLACE INTO s4_classrep.equiclass_distbd BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_distribution_board t;

-- # DMONAP Alignment Pins (no ai2 match)
-- # DMONCM Crack Monitoring Point (no ai2 match)

-- # DMONEX Extensometer
INSERT OR REPLACE INTO s4_classrep.equiclass_dmonex BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_extensometer t;

-- # DMONHP Hanging Plum Bob
INSERT OR REPLACE INTO s4_classrep.equiclass_dmonhp BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_hanging_plum_bob t;

-- # DMONIN Inclinometer
INSERT OR REPLACE INTO s4_classrep.equiclass_dmonin BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_inclinometer t;

-- # DMONIP Inverted Plum Bob
INSERT OR REPLACE INTO s4_classrep.equiclass_dmonip BY NAME
SELECT
    t.equipment_id AS equipment_id, 
FROM ai2_classrep.equiclass_inverted_plum_bob t;

-- # DMONLP Levelling Pins (no ai2 match)
-- # DMONSC Survey Control Point (no ai2 match)
-- # DMONSP Survey Points (no ai2 match)

-- # DRANDR Drain
INSERT OR REPLACE INTO s4_classrep.equiclass_drandr BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_drain t;

-- # DRANMISC Miscellaneous Drain
-- # DRANMT Mitre Drain
-- # DRANPR Pressure Relief Well
-- # DRANSO Soakaway
-- # DRANTO Toe Drain
-- # EMTRBK Brake Motor
-- # EMTRDC Direct Current Motor

-- EMTRIN
INSERT OR REPLACE INTO s4_classrep.equiclass_emtrin BY NAME
SELECT
    t.equipment_id AS equipment_id,   
    t._insulation_class AS insulation_class_deg_c, 
    t._ip_rating AS ip_rating,
    t._location_on_site AS location_on_site,
    TRY_CAST(t._current_in AS DECIMAL) AS emtr_rated_current_a,
    udf_power_to_killowatts(t._power_units, t._power) AS emtr_rated_power_kw,
    udf_voltage_ac_or_dc(t._voltage_in_ac_or_dc) AS emtr_rated_voltage_units,
    TRY_CAST(t._voltage_in AS INTEGER) AS emtr_rated_voltage,
FROM ai2_classrep.equiclass_non_immersible_motor t;


-- # EMTRMISC Miscellaneous Electric Motor
-- # EMTRSR Slip Ring Motor
-- # EMTRSU Submersible Motor
-- # ETLYOS OSEC Electrolyser 
-- # FANSAX Axial Fan
-- # FANSCE Centrifugal Fan
-- # FANSMISC Miscellaneous Fan
-- # FCDE Flow Control Device
-- # FCDEMISC Miscellaneous Flow Control Device
-- # FCDEOF Orifice Flow Control Device
-- # FCDEVX Vortex Flow Control Device
-- # FIDAMP Fire Damper
-- # FIDANR Non Return Damper
-- # FIDASO Shut Off Damper
-- # FLAMAR Flame Arrestor
-- # FLSTCL Channel
-- # FLSTFL Flow Weir
-- # FLSTFM Flume
-- # FLSTMISC Miscellaneous Flow Structure
-- # FLSTVN V Notch Weir
-- # FLYWHE Flywheel
-- # FSTNCF Coriolis Effect Flow Device
-- # FSTNEM Electromagnetic Flow Device
-- # FSTNIP Emag Insertion Probe Flow Device
-- # FSTNLS Laser Surface Effect Flow Device
-- # FSTNME Mechanically Actuated Flow Switch
-- # FSTNMISC Miscellaneous Flow Transmitter
-- # FSTNOC Open Channel (usonic) Flow Device
-- # FSTNOP Orifice plate (DP) Flow Device
-- # FSTNTH Thermal Flow Transmitter
-- # FSTNTM Thermal Mass Flow Transmitter
-- # FSTNTU Rotating Turbine Flow Device
-- # FSTNUS Closed Pipe Doppler Flow Device
-- # FSTNVA Variable Area Flow Device
-- # FSTNVE Venturi Effect Flow Device
-- # FSTNVO Vortex Effect Flow Device
-- # FSTNWS Wind Speed Device
-- # FTUNFC Filter Element
-- # FTUNMISC Miscellaneous Filter Unit
-- # FTUNRJ Reverse Jet Filter