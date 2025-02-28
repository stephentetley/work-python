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
INSERT OR REPLACE INTO s4_classrep.equiclass_emtrdc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_d_c_motor t;

-- EMTRIN
INSERT OR REPLACE INTO s4_classrep.equiclass_emtrin BY NAME
SELECT
    t.equipment_id AS equipment_id,   
    t._insulation_class AS insulation_class_deg_c, 
    t._ip_rating AS ip_rating,
    t._location_on_site AS location_on_site,
    TRY_CAST(t._current_in AS DECIMAL) AS emtr_rated_current_a,
    udfx.power_to_killowatts(t._power_units, t._power) AS emtr_rated_power_kw,
    udfx.get_voltage_ac_or_dc(t._voltage_in_ac_or_dc) AS emtr_rated_voltage_units,
    TRY_CAST(t._voltage_in AS INTEGER) AS emtr_rated_voltage,
FROM ai2_classrep.equiclass_non_immersible_motor t;


-- # EMTRMISC Miscellaneous Electric Motor
-- # EMTRSR Slip Ring Motor
INSERT OR REPLACE INTO s4_classrep.equiclass_emtrsr BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_slip_ring_motor t;

-- # EMTRSU Submersible Motor
INSERT OR REPLACE INTO s4_classrep.equiclass_emtrsu BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_submersible_motor t;

-- # ETLYOS OSEC Electrolyser 
-- # FANSAX Axial Fan
-- # FANSCE Centrifugal Fan
-- # FANSMISC Miscellaneous Fan
-- # FCDE Flow Control Device
-- # FCDEMISC Miscellaneous Flow Control Device
-- # FCDEOF Orifice Flow Control Device

-- # FCDEVX Vortex Flow Control Device
INSERT OR REPLACE INTO s4_classrep.equiclass_fcdevx BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_vortex_fcd t;

-- # FIDAMP Fire Damper
-- # FIDANR Non Return Damper
-- # FIDASO Shut Off Damper
-- # FLAMAR Flame Arrestor
-- # FLSTCL Channel
-- # FLSTFL Flow Weir
-- # FLSTFM Flume
INSERT OR REPLACE INTO s4_classrep.equiclass_flstfm BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_flume t;

-- # FLSTMISC Miscellaneous Flow Structure
-- # FLSTVN V Notch Weir
-- # FLYWHE Flywheel
INSERT OR REPLACE INTO s4_classrep.equiclass_flywhe BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_flywheel t;

-- # FSTNCF Coriolis Effect Flow Device

-- # FSTNEM Electromagnetic Flow Device
INSERT OR REPLACE INTO s4_classrep.equiclass_fstnem BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udfx.format_signal3(t._signal_min, t._signal_max, t._signal_unit) AS fstn_signal_type,
    t._range_min AS fstn_range_min,
    t._range_max AS fstn_range_max,
    upper(t._range_unit) AS fstn_range_units,
    t._pipe_diameter_mm AS fstn_pipe_diameter_mm,
    t._sensor_calibration_factor_1 AS fstn_sens_calibration_factor_1,
    t._sensor_calibration_factor_2 AS fstn_sens_calibration_factor_2,
    t._sensor_calibration_factor_3 AS fstn_sens_calibration_factor_3,
    t._sensor_calibration_factor_4 AS fstn_sens_calibration_factor_4,
FROM ai2_classrep.equiclass_magnetic_flow_instrument t;

-- # FSTNIP Emag Insertion Probe Flow Device

-- # FSTNLS Laser Surface Effect Flow Device
INSERT OR REPLACE INTO s4_classrep.equiclass_fstnls BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_laser_surface_effect_flow_instrument t;

-- # FSTNME Mechanically Actuated Flow Switch
-- # FSTNOC Open Channel (usonic) Flow Device
INSERT OR REPLACE INTO s4_classrep.equiclass_fstnoc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ultrasonic_flow_instrument t;

-- # FSTNOP Orifice plate (DP) Flow Device

-- # FSTNTH Thermal Flow Transmitter
INSERT OR REPLACE INTO s4_classrep.equiclass_fstnth BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_thermal_flow_instrument t;

-- # FSTNTM Thermal Mass Flow Transmitter
INSERT OR REPLACE INTO s4_classrep.equiclass_fstntm BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_thermal_mass_flow_instrument t;

-- # FSTNTU Rotating Turbine Flow Device
INSERT OR REPLACE INTO s4_classrep.equiclass_fstntu BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_turbine_flow_instrument t;

-- # FSTNUS Closed Pipe Doppler Flow Device

-- # FSTNVA Variable Area Flow Device
INSERT OR REPLACE INTO s4_classrep.equiclass_fstnva BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_variable_area_flow_instrument t;

-- # FSTNVE Venturi Effect Flow Device

-- # FSTNVO Vortex Effect Flow Device
INSERT OR REPLACE INTO s4_classrep.equiclass_fstnvo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_vortex_flow_instrument t;

-- # FSTNWS Wind Speed Device
-- # FTUNFC Filter Element
-- # FTUNMISC Miscellaneous Filter Unit
-- # FTUNRJ Reverse Jet Filter