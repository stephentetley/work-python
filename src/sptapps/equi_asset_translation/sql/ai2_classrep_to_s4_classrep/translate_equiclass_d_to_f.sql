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

-- # DAMSCO Dam Core
-- # DAMSCR Dam Crest
-- # DAMSCT Cut off Trench
-- # DAMSDM Dam
-- # DAMSFD Dam Face Downstream
-- # DAMSFP Dam Face Upstream
-- # DAMSMISC Miscellaneous Dam Structure
-- # DAMSSB Stilling Basin
-- # DAMSTB Tumble Bay
-- # DAMSTU Tunnel
-- # DAMSWW Wave Wall
-- # DECOEB Emergency Eye Wash Station
-- # DECOES Emergency Eye Wash and Shower
-- # DECOMISC Miscellaneous Decontamination Unit
-- # DECOSH Emergency Shower
-- # DECTEX Gas Detector for Explosive Atmospheres
-- # DECTFM Detector for boiler flame sources
-- # DECTMISC Miscellaneous Detection Device
-- # DECTPA Monitor Panel for Gas Detectors
-- # DECTTX Gas Detector for Toxic Atmospheres
-- # DESTOZ Ozone Destructor
-- # DHUMAC Air Conditioning Dehumidifier
-- # DHUMDE Desiccant Dehumidifier
-- # DHUMMISC Miscellaneous Dehumidifier
-- # DISTBD Distribution Board
-- # DMONAP Alignment Pins
-- # DMONCM Crack Monitoring Point
-- # DMONEX Extensometer
-- # DMONHP Hanging Plum Bob
-- # DMONIN Inclinometer
-- # DMONIP Inverted Plum Bob
-- # DMONLP Levelling Pins
-- # DMONMISC Miscellaneous Deformation Monitor
-- # DMONSC Survey Control Point
-- # DMONSP Survey Points
-- # DRANDR Drain
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