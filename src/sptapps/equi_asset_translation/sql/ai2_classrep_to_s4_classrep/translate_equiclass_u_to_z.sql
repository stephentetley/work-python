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
INSERT OR REPLACE INTO s4_classrep.equiclass_valvav BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_air_relief_valve t;

-- # VALVBA Ball Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvba BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_isolating_valves t
WHERE upper(t._valve_type) = 'BALL';

-- # VALVBE Bellmouth Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvbe BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_bellmouth_valve t;

-- # VALVBP Butterfly Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvbp BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_isolating_valves t
WHERE upper(t._valve_type) = 'BUTTERFLY';

-- # VALVDC Disc Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvdc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_disc_valve t;

-- # VALVDE Decant Valve
-- # VALVDI Diaphragm Valve
-- # VALVDO Dome Valve
-- # VALVDR Directional Control Valve

-- # VALVFL Flap Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvfl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_flap_valve t;

-- # VALVFT Float Operated Valve

-- # VALVFU Flush Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvfu BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_auto_flushing_valve t;

-- # VALVGA Gate Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvga BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_isolating_valves t
WHERE upper(t._valve_type) IN ('KNIFE GATE', 'WEDGE GATE');

-- # VALVGL Globe Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvgl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_globe_valve t;

-- # VALVLO Loading Valve
-- # VALVMISC Miscellaneous Valve

-- # VALVMW Multi Way Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvmw BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_multi_way_valve t;

-- # VALVNE Needle Valve
-- # VALVNR Non Return Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvnr BY NAME
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_non_return_valve t)
UNION
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_non_return_valve_clean t)
UNION
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_non_return_valve_waste t)
;

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
INSERT OR REPLACE INTO s4_classrep.equiclass_valvre BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_bore_diameter_mm,
FROM ai2_classrep.equiclass_pressure_regulating_valve t;


-- # VALVSC Pressure Sustaining Valve
INSERT OR REPLACE INTO s4_classrep.equiclass_valvsc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_size_to_millimetres(t._size_units, t._size) AS valv_inlet_size_mm,
FROM ai2_classrep.equiclass_press_sustaining_valve t;

-- # VALVSF Safety Pressure Relief Valve

-- # VALVSL Solenoid Valve
-- # VALVVI Vacuum Interface Valve
-- # VALVVR Vacuum Regulator Valve
-- # VEGRAG Air Grate
-- # VEPRAC Autoclave

-- # VEPRAO Air and Oil Receiver Vessel
INSERT OR REPLACE INTO s4_classrep.equiclass_veprao BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    t._ywref AS statutory_reference_number,
FROM ai2_classrep.equiclass_oil_air_receiver t;

-- # VEPRAR Air Receiver Vessel
INSERT OR REPLACE INTO s4_classrep.equiclass_veprar BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    t._ywref AS statutory_reference_number,
FROM ai2_classrep.equiclass_air_receiver t;

-- # VEPRAW Air and Water Receiver Vessel
INSERT OR REPLACE INTO s4_classrep.equiclass_vepraw BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    t._ywref AS statutory_reference_number,
FROM ai2_classrep.equiclass_water_air_receiver t;

-- # VEPRFI Pressurised Filter
-- # VEPRGH Gas Holder
-- # VEPRMISC Miscellaneous Pressure Vessel
-- # VEPRNW Nitrogen and Water Receiver Vessel
-- # VEPROZ Ozone Generator
INSERT OR REPLACE INTO s4_classrep.equiclass_veproz BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    t._ywref AS statutory_reference_number,
FROM ai2_classrep.equiclass_ozone_generator t;

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
INSERT OR REPLACE INTO s4_classrep.equiclass_welldr BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_dry_well t;

-- # WELLMISC Miscellaneous Well

-- # WELLWT Wet Well
INSERT OR REPLACE INTO s4_classrep.equiclass_wellwt BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_wet_well t;

-- # WETNSG Strain Gauge Load Cell Device
-- # WWC Waste Water Crossing
