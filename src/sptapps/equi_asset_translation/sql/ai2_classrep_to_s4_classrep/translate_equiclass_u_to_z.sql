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

-- # UVUNIT Ultraviolet Unit
-- # VALVAS Angle Seat Valve
-- # VALVAV Air Release Valve
-- # VALVBA Ball Valve
-- # VALVBE Bellmouth Valve
-- # VALVBP Butterfly Valve
-- # VALVDC Disc Valve
-- # VALVDE Decant Valve
-- # VALVDI Diaphragm Valve
-- # VALVDO Dome Valve
-- # VALVDR Directional Control Valve
-- # VALVFL Flap Valve
-- # VALVFT Float Operated Valve
-- # VALVFU Flush Valve
-- # VALVGA Gate Valve
-- # VALVGL Globe Valve
-- # VALVLO Loading Valve
-- # VALVMISC Miscellaneous Valve
-- # VALVMW Multi Way Valve
-- # VALVNE Needle Valve
-- # VALVNR Non Return Valve
-- # VALVPB Pressure Intensifier Booster

-- # VALVPE Penstock Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvpe BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    t._penstock_width_mm AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_penstock t;


-- # VALVPG Plug Valve
-- # VALVPI Pinch Valve
-- # VALVPR Pressure Reducing Valve
-- # VALVPV Pressure and Vacuum Relief Valve
-- # VALVRA Ram Valve
-- # VALVRE Pressure Regulating Valve
-- # VALVSC Pressure Sustaining Valve
-- # VALVSF Safety Pressure Relief Valve
-- # VALVSL Solenoid Valve
-- # VALVVI Vacuum Interface Valve
-- # VALVVR Vacuum Regulator Valve
-- # VEGRAG Air Grate
-- # VEPRAC Autoclave
-- # VEPRAO Air and Oil Receiver Vessel
-- # VEPRAR Air Receiver Vessel
-- # VEPRAW Air and Water Receiver Vessel
-- # VEPRFI Pressurised Filter
-- # VEPRGH Gas Holder
-- # VEPRMISC Miscellaneous Pressure Vessel
-- # VEPRNW Nitrogen and Water Receiver Vessel
-- # VEPROZ Ozone Generator
-- # VEPRPA Pasteurisation Vessel
	


-- # VEPRPD Pulsation Damper
INSERT OR REPLACE INTO s4_classrep.equiclass_veprpd BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    t._ywref AS statutory_reference_number,
FROM ai2_classrep.equiclass_pulsation_damper t;


-- # VEPRPR Pressure Vessel
-- # VEPRSB Steam Boiler
-- # VEPRSS Steam System
-- # VEPRVA Vacuum Vessel
-- # VESLBB Biogas Bag
-- # VESLBH Biogas Holder
-- # VESLMISC Miscellaneous Vessel
-- # VIBRUN Vibration Unit
-- # WACAPC Pressure Wash Carriage
-- # WASFTN Water Softener
-- # WASHMISC Miscellaneous Pressure Washer
-- # WASHPR Pressure Washer
-- # WASHWH Wheel Washer
-- # WBRDGE Weighbridge
-- # WEIRMISC Miscellaneous Weir
-- # WEIROF Weir Overflow
-- # WELLDR Dry Well
-- # WELLMISC Miscellaneous Well
-- # WELLWT Wet Well
-- # WETNSG Strain Gauge Load Cell Device
-- # WWC Waste Water Crossing
