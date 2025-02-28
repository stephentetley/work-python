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

-- # GASWCP Capacitive Effect Proximity Switch
-- # GASWIP Inductive Effect Proximity Switch
-- # GASWME Mechanical Interlock Position Switch
-- # GASWMG Magnetic Interlock Position Switch
-- # GASWMISC Miscellaneous Position Sensor
-- # GASWOP Light Reflection Position Switch
-- # GASWUS Ultrasonic Position Switch
-- # GATNMISC Miscellaneous Linear Move Sensor
-- # GATNOD Gauge Transmitter with Optical Detector
-- # GATNPO Potentiometer Based Gauge Transmitter
-- # GATNPS Gauge Transmitter for Pulse Sequences
-- # GATNUS Ultrasonic Motion Transmitter
-- # GCHPNL Gas Changeover Panel
-- # GMTRGM Geared Motor
INSERT OR REPLACE INTO s4_classrep.equiclass_gmtrgm BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_geared_motor t;

-- # GMTRMISC Miscellaneous Geared Motor
-- # GRICMISC Miscellaneous Grit Cyclon Separator
-- # GRICRA Rake Classifier
-- # GRICSC Screw Classifier
-- # GRISCF Cross Flow Grit Separator
-- # GRISMISC Miscellaneous Grit Separator
-- # GRISVF Vortex Flow Grit Separator
-- # GRNDER Grinder
-- # GSDOVS Vacuum Gas Feed System
-- # HEATAI Air Heater
-- # HEATBO Boiler
-- # HEATEV Heater Evaporator

-- # HEATFA Fan Heater
INSERT OR REPLACE INTO s4_classrep.equiclass_heatfa BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_fan_heater t;

-- # HEATGS Gas Heater

-- # HEATIM Immersion Heater
INSERT OR REPLACE INTO s4_classrep.equiclass_heatim BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_immersion_heater t;

-- # HEATMISC Miscellaneous Heating Unit
-- # HEATOI Oil Heater
-- # HEATRA Heating Radiator
-- # HEATSH Storage Heater

-- # HEATTR Trace Heating
INSERT OR REPLACE INTO s4_classrep.equiclass_heattr BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_trace_heaters t;

-- # HEATTU Tubular Heater
INSERT OR REPLACE INTO s4_classrep.equiclass_heattu BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_tubular_heater t;

-- # HEEXCH Heat Exchanger
-- # HEPLBH Borehole Head Plate
-- # HIWYBP Bridle Path
-- # HIWYMISC Miscellaneous Highways and roads
-- # HIWYPP Path
-- # HIWYRD Road
INSERT OR REPLACE INTO s4_classrep.equiclass_hiwyrd BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_road t;

-- # HSYSPP Hydraulic Power Pack
INSERT OR REPLACE INTO s4_classrep.equiclass_hsyspp BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hydraulic_power_pack t;

-- # INTCFG Fat Oil and Grease Interceptor
INSERT OR REPLACE INTO s4_classrep.equiclass_intcfg BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_interceptor t;

-- # INTFCH Chart Recorder Based Interface
-- # INTFCO Interface Device converting Signals
-- # INTFDI Interface Device with Digital Readout
-- # INTFLO Local Operator Interface for PLCs
INSERT OR REPLACE INTO s4_classrep.equiclass_intflo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_l_o_i t;

-- # INTFMISC Miscellaneous Interface
-- # INTFSC Interface Display Node for SCADA System X
-- # INTJLA Injector lance
-- # JACKHY Hydraulic Jack
INSERT OR REPLACE INTO s4_classrep.equiclass_jackhy BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_jack_hydraulic t;

-- # JACKRA Ratchet Jack
INSERT OR REPLACE INTO s4_classrep.equiclass_jackra BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_jack_ratchet t;

-- # KISKKI Kiosk
INSERT OR REPLACE INTO s4_classrep.equiclass_kiskki BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    null AS location_on_site,
    udfx.size_to_millimetres('METRES', t._kiosk_base_height_m) AS kisk_base_height_mm,
    udfx.size_to_millimetres('METRES', t._kiosk_depth_m) AS kisk_depth_mm,
    udfx.size_to_millimetres('METRES', t._kiosk_height_m) AS kisk_height_mm,
    udfx.size_to_millimetres('METRES', t._kiosk_width_m) AS kisk_width_mm,
    t._kiosk_material AS kisk_material,
FROM ai2_classrep.equiclass_kiosk t;
