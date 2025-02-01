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

-- TODO should we record the source table?

-- # ACSTBO Boardwalk Access Structure 
INSERT OR REPLACE INTO s4_classrep.equiclass_acstbo BY NAME
SELECT
    t.equipment_id AS equipment_id,
FROM ai2_classrep.equiclass_boardwalk t;

-- # ACSTBR Bridge
INSERT OR REPLACE INTO s4_classrep.equiclass_acstbr BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_bridge t;

-- # ACSTLA Ladders
INSERT OR REPLACE INTO s4_classrep.equiclass_acstla BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_ladders t;

-- # ACSTRM Ramp
INSERT OR REPLACE INTO s4_classrep.equiclass_acstrm BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_ramp t;

-- # ACSTST Stairs
INSERT OR REPLACE INTO s4_classrep.equiclass_acstst BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_steps_or_stairs_or_stair_case t;


-- # ACTUEH Hydraulic Actuator
INSERT OR REPLACE INTO s4_classrep.equiclass_actueh BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hydraulic_actuator t;

-- # ACTUEM Electric Motor Actuator
INSERT OR REPLACE INTO s4_classrep.equiclass_actuem BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_actuator t;

-- # ACTUEP Pneumatic Actuator
INSERT OR REPLACE INTO s4_classrep.equiclass_actuep BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_pneumatic_actuator t;

-- # AERAHA Horizontal Aeration Unit (no ai2 match)

-- # AERASD Submerged Aeration Diffuser (no ai2 match)

-- # AERAVA Vertical Aeration Unit (no ai2 match)

-- # AGITAG Agitation or Vibration Device (no ai2 match)

-- # AIRCON Air Conditioning Unit
INSERT OR REPLACE INTO s4_classrep.equiclass_aircon BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_air_conditioning_system t;

-- # AIRDDU Dessicant Air Drying Unit (no ai2 match)

-- # AIRDRU Refrigerant Air Drying Unit (no ai2 match)

-- # ALAMCC CCTV Camera
INSERT OR REPLACE INTO s4_classrep.equiclass_alamcc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_cctv_equipment t;


-- # ALAMFS Fire Alarm System
INSERT OR REPLACE INTO s4_classrep.equiclass_alamfs BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_fire_alarm t;

-- # ALAMIA Intruder Alarm
INSERT OR REPLACE INTO s4_classrep.equiclass_alamia BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_burglar_alarm t;

-- # ANALAL Aluminium (Ion) Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analal BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_aluminium_instrument t;

-- # ANALAM Ammonia (Ion) Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analam BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ammonia_instrument t;

-- # ANALCD Chemical Oxygen Demand Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analcd BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_oxygen_instrument t;

-- # ANALCL Chlorine (Ion) Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analcl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_chlorine_instrument t;

-- # ANALCO Conductivity (in fluid) Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analco BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_conductivity_instrument t;

-- # ANALDE Density Analyser 
INSERT OR REPLACE INTO s4_classrep.equiclass_analde BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_density_instrument t;


-- # ANALDO Dissolved Oxygen Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analdo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_dissolved_oxygen_instrument t;

-- # ANALFE Iron (Ion) Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analfe BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_iron_instruments t;

-- # ANALFG Flue Gas Emissions Analyser (no ai2 match)

-- # ANALFS Analyser Flow Cell (no ai2 match)

-- # ANALGA Gas Composition Analyser (no ai2 match)

-- # ANALGC CO Gas Analyser Module
INSERT OR REPLACE INTO s4_classrep.equiclass_analgc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_carbon_monoxide_gas_instrument t;

-- # ANALGE H2S Gas Analyser Module
INSERT OR REPLACE INTO s4_classrep.equiclass_analge BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hydrogen_sulphide_instrument t;

-- # ANALGH H2 Gas Analyser Module
INSERT OR REPLACE INTO s4_classrep.equiclass_analgh BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hydrogen_gas_instrument t;

-- # ANALGM CH4 Gas Analyser Module
INSERT OR REPLACE INTO s4_classrep.equiclass_analgm BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_methane_gas_instrument t;

-- # ANALGS SO2 Gas Analyser Module
INSERT OR REPLACE INTO s4_classrep.equiclass_analgs BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_sulphur_dioxide_gas_instrument t;

-- # ANALHA Water Hardness Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analha BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_water_hardness_instrument t;

-- # ...


-- # BARREP Edge Protection
INSERT OR REPLACE INTO s4_classrep.equiclass_barrep BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_edge_protection t;

-- # BARRFN Fence
INSERT OR REPLACE INTO s4_classrep.equiclass_barrfn BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_fence t;

-- # BARRWA Wall (no ai2 match)

-- # BHLEWL Borehole Well (no ai2 match)

-- # BIOFLD Linear Distributor (no ai2 match)

-- # BIOFRD Rotary Distributor (no ai2 match)

-- # BIOFTD Tipper Distributor (no ai2 match)

-- # BLDGBU Building (no ai2 match)

-- # BLOWAB Air Blast Cleaning System (no ai2 match)

-- # BLOWCB Centrifugal Blower (no ai2 match)

-- # BLOWHB Hybrid Blower (no ai2 match)

-- # BLOWRC Rotary Claw Blower (no ai2 match)

-- # BLOWRL Rotary Lobe Blower (no ai2 match)

-- # BLOWRV Rotary Vane Blower (no ai2 match)

-- # BLOWSC Side Channel Blower (no ai2 match)

-- # BSINDB Detention Basin
INSERT OR REPLACE INTO s4_classrep.equiclass_bsindb BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_detention_basin_dry_pond t;

-- # BSINRB Retention Basin
INSERT OR REPLACE INTO s4_classrep.equiclass_bsinrb BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_retention_basin_wet_pond t;

-- # BURNGA Gas Burner (no ai2 match)

-- # BURNOI Oil Burner (no ai2 match)

-- # BUSBUN Busbar Unit
INSERT OR REPLACE INTO s4_classrep.equiclass_busbun BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_bus_bar_unit t;

-- # CARPRK Car Park
INSERT OR REPLACE INTO s4_classrep.equiclass_carprk BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_car_park t;

-- # CASEBH Borehole Lining
INSERT OR REPLACE INTO s4_classrep.equiclass_casebh BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_borehole_lining t;

-- # CATHPR Cathodic Protection
INSERT OR REPLACE INTO s4_classrep.equiclass_cathpr BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_cathodic_protection t;


-- # CCBKAC Air Circuit Breaker (two ai2 tables)
INSERT OR REPLACE INTO s4_classrep.equiclass_ccbkac BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_air_circuit_breaker t;

INSERT OR REPLACE INTO s4_classrep.equiclass_ccbkac BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hv_air_circuit_breaker t;

-- # CCBKMC Moulded Case Circuit Breaker
INSERT OR REPLACE INTO s4_classrep.equiclass_ccbkmc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_moulded_case_circuit_breaker_mccb t;

-- # CCBKMI Miniature Circuit Breaker (no ai2 match)

-- # CCBKOC Oil Circuit Breaker (two ai2 tables)
INSERT OR REPLACE INTO s4_classrep.equiclass_ccbkoc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_oil_circuit_breaker t;

INSERT OR REPLACE INTO s4_classrep.equiclass_ccbkoc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hv_oil_circuit_breaker t;

-- # CCBKRC Residual Current Circuit Breaker (no ai2 match)


-- # CCBKSF SF6 Circuit Breaker
INSERT OR REPLACE INTO s4_classrep.equiclass_ccbksf BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hv_sf6_circuit_breaker t;

-- # CCBKVC Vacuum Circuit Breaker (two ai2 tables)
INSERT OR REPLACE INTO s4_classrep.equiclass_ccbkvc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_vacuum_circuit_breaker t;

INSERT OR REPLACE INTO s4_classrep.equiclass_ccbkvc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hv_vacuum_circuit_breaker t;


-- # CENGGE Gas Engine
INSERT OR REPLACE INTO s4_classrep.equiclass_cengge BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_gas_powered_generators t;


-- # CONPNL Control Panel
INSERT OR REPLACE INTO s4_classrep.equiclass_conpnl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    TRY_CAST(t._current_in AS DECIMAL) AS conp_rated_current_a,
    udf_voltage_ac_or_dc(t._voltage_in_ac_or_dc) AS conp_rated_voltage_units,
    TRY_CAST(t._voltage_in AS INTEGER) AS conp_rated_voltage,
FROM ai2_classrep.equiclass_control_panel t;

-- # CONPPN Pneumatic Control Panel
INSERT OR REPLACE INTO s4_classrep.equiclass_conppn BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_pneumatic_control_panel t;