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



-- # ACSTRM Ramp
INSERT OR REPLACE INTO s4_classrep.equiclass_acstrm BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_ramp t;

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


-- # ANALDO Dissolved Oxygen Analyser
INSERT OR REPLACE INTO s4_classrep.equiclass_analdo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_dissolved_oxygen_instrument t;

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



-- # CCBKMI Miniature Circuit Breaker (no ai2 table)

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

-- # CCBKRC Residual Current Circuit Breaker (no ai2 table)


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