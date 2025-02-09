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


-- # PEIZHY Hydraulic Type Piezometer
-- # PEIZMISC Miscellaneous Piezometer
-- # PEIZPN Pneumatic Type Piezometer
-- # PEIZVB Vibrating Wire Piezometer
-- # PELTER Pelletiser
-- # PIPEMISC Miscellaneous Pipework
-- # PIPERM Rising Main
INSERT OR REPLACE INTO s4_classrep.equiclass_piperm BY NAME
(SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_borehole_rising_main t)
UNION
(SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_sewerage_rising_main t)
;

-- # PM_PLANNING_ATTRIB PM: Planning Attibutes
-- # PM_RPN PM: Risk Priority Number
-- # POCPPA Power Changeover Panel
-- # PODEBA Battery Storage
-- # PODEBC Battery Charger
INSERT OR REPLACE INTO s4_classrep.equiclass_podebc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_battery_charger t;

-- # PODEDL DC Power Supply for ELV Circuits
-- # PODEMISC Miscellaneous Power Device
-- # PODEPC Phase Converter
-- # PODEPF Power Supply Filter
-- # PODETU Telemetry Uninterruptable Power Supply
-- # PODEUP Uninterruptable Power Supply
-- # POFCCP Power Factor Capacitor
-- # POFCMISC Miscellaneous Power Factor Correct
-- # POGEAC Alternator
-- # POGEMISC Miscellaneous Power Generator
-- # POGESP Solar Photovoltaic Panel

-- # PROFAC Activated Carbon Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profac BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_cw_activated_carbon_filter t;

-- # PROFBA Biological Aeration Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profba BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ww_biological_aeration_filter t;

-- # PROFCF Continuous Flow Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profcf BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ww_continuous_flow_filter t;

-- # PROFCT Contact Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profct BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_cw_contact_filter t;

-- # PROFCW Constructed Wetland

-- # PROFDM Drum Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profdm BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_drum_filter t;

-- # PROFHR High Rate Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profhr BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ww_high_rate_filter t;

-- # PROFME Membrane Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profme BY NAME
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_cw_membrane_filter t)
UNION
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ww_membrane_filter t)
;

-- # PROFMISC Miscellaneous Process Filter

-- # PROFNI Nitrifying Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profni BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ww_nitrifying_filter t;

-- # PROFPA Particle Filter
-- # PROFPC Percolating Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profpc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ww_percolating_filter t;

-- # PROFPR Pressure Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profpr BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_cw_pressure_filter t;

-- # PROFRB WW RBC(Bio Disc)

-- # PROFRG Rapid Gravity Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profrg BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_cw_rapid_gravity_filter t;

-- # PROFSA Submerged Aerated Process Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profsa BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ww_submerged_aerated_filter t;

-- # PROFSN Sand Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profsn BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ww_sand_filter t;

-- # PROFSS Slow Sand Filter
INSERT OR REPLACE INTO s4_classrep.equiclass_profss BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_cw_slow_sand_filter t;

-- # PSTNDI Diaphragm Type Pressure Device
-- # PTDESP Surge Protection Unit
-- # PUMPAX Axial Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumpax BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_axial_pump t;

-- # PUMPCE Centrifugal Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumpce BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_centrifugal_pump t;

-- # PUMPDI Diaphragm Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumpdi BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_diaphragm_pump t;

-- # PUMPEJ Ejector Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumpej BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ejector_pump t;

-- # PUMPGE Gear Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumpge BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_gear_pump t;

-- # PUMPHR Helical Rotor Pump
-- # PUMPLO Lobe Pump
-- # PUMPLU Lubrication Pump
-- # PUMPMA Maceration Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumpma BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_macipump t;

-- # PUMPMISC Miscellaneous Pump
-- # PUMPPB Peristaltic Buffer Pump
-- # PUMPPE Peristaltic Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumppe BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_peristaltic_pump t;

-- # PUMPPG Plunger Pump
-- # PUMPRA Ram Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumpra BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ram_pump t;

-- # PUMPSC Screw Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumpsc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_screw_pump t;

-- # PUMPVA Vacuum Pump
-- # PUMSBH Borehole Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumsbh BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_borehole_pump t;

-- # PUMSMISC Miscellaneous Submersible Pump
-- # PUMSMO Submersible Pump with Integral Motor
INSERT OR REPLACE INTO s4_classrep.equiclass_pumsmo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_submersible_centrifugal_pump t
WHERE t._integral_motor_y_n = 'YES';

-- # PUMSSU Submersible Pump
INSERT OR REPLACE INTO s4_classrep.equiclass_pumssu BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_submersible_centrifugal_pump t
WHERE t._integral_motor_y_n = 'NO';

-- # REATRE Reactor
-- # RELYMISC Miscellaneous Relay
-- # RELYPF Phase Failure Relay
-- # RESVMISC Miscellaneous Reservoir
-- # RESVOW Open Water Reservoir
-- # SAFETY_CRIT_EQ Safety Critical Equipment
-- # SALTSA Salt Saturator
-- # SAMPCH Refrigerated Unit for Liquid Samples
-- # SAMPCY Unit to store Cryptosporidium Samples
-- # SAMPMISC Miscellaneous Sampler
-- # SAMPPB Plumbosolvency Sampler Unit
-- # SCPRLS Linear Scraper
-- # SCPRMISC Miscellaneous Scraper Bridge
-- # SCPRRO Rotary Scraper
-- # SCRCBA Bar
-- # SCRCMISC Miscellaneous Coarse Screen
-- # SCRFBA Band
-- # SCRFBM Brush Screen with Macerator
-- # SCRFBR Brush Screen
-- # SCRFDC Rotating Drum or Cup
-- # SCRFES Escalator or Fine Screen
-- # SCRFHS Horizontal Spiral Brush
-- # SCRFMISC Miscellaneous Fine Screen
-- # SCRFRB Raked Bar
-- # SCRFRC Rotating Drum or Cup with Compactor
-- # SCRFRS Replaceable Screening Sack
-- # SCRFSC Spiral Brush with Compactor
-- # SCRFSE Stepped Bar
-- # SCRFST Static Screen
-- # SCRFVS Vertical Spiral Brush
-- # SCUMSY Scum Removal System
-- # SECDGR Grill
-- # SECDMISC Miscellaneous Security Device
-- # SECDRD Roller Shutter Door
-- # SECDSG Security Gate
-- # SFERBA Breathing Apparatus Cylinder
-- # SFEREB Emergency Breathing Device
-- # SFERFA Fall Arrester
-- # SFERFH Fall Arrester Harness
-- # SFERFT Fall Arrester Traveller
-- # SFERLB Life Buoy
-- # SFERLS Safety Lanyard
-- # SFERMISC Miscellaneous Safety and Rescue
-- # SFERPB Portable Breathing Apparatus
-- # SFERPL Anchor Plate
-- # SFERPO Anchor Point
-- # SFERTR Tripod
-- # SGERBP Bypass Switch
-- # SGERCF Contactor Feeder
-- # SGERCO Changeover Switch
-- # SGEREI Electrical Isolation Unit
-- # SGERFC Fuse Connection Unit
-- # SGERFS Fused Switch
INSERT OR REPLACE INTO s4_classrep.equiclass_sgerfs BY NAME
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_fuse_switch t)
UNION
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hv_switch_fuse t);

-- # SGERIS Isolator Switch
INSERT OR REPLACE INTO s4_classrep.equiclass_sgeris BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_isolators_switches t;

-- # SGERMISC Miscellaneous Switchgear
-- # SGERNE Neutral Earth Contactor

-- # SGEROS Oil Switch
INSERT OR REPLACE INTO s4_classrep.equiclass_sgeros BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hv_oil_switch t;

-- # SGERRM Ring Main Unit

-- # SGERSF SF6 Switch
INSERT OR REPLACE INTO s4_classrep.equiclass_sgersf BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_hv_sf6_switch t;

-- # SHCFBM Shape Circular Flat Bottom
-- # SHCOBM Shape Circular Conical Bottom
-- # SHECYL Shape Elliptical Cyl (Horiz)
-- # SHEWIN Inlet
-- # SHEWOT Outlet
-- # SHHCYL Shape Horizontal Cylinder
-- # SHMIIR Shape Miscellaneous Irregular
-- # SHRFBM Shape Rectangular Flat Bottom
-- # SHRPBM Shape Rect Pyramid Bottom
-- # SHRSBM Shape Rect Sloping Bottom
-- # SHSCYL Shape Semi-cylinder
-- # SHWRDE Domestic Electrical Shower
-- # SIGNSA Safety Sign
-- # SILOXX Silo
-- # SKIMDE Decanter Skimmer
-- # SKIMLN Linear Skimmer
-- # SLABSL Slab
-- # SLIPLN Linear Slide Assembly
-- # SLIPRA Slip Ring Assembly
-- # SLPRBE Belt Press
INSERT OR REPLACE INTO s4_classrep.equiclass_slprbe BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_belt_press t;

-- # SLPRMISC Miscellaneous Sludge Press
-- # SLPRPL Plate Press
-- # SOCKET Panel Socket
-- # SOCKGE Generator Connection
-- # SOLEEM Solenoid Electromagnetic
-- # SOLUTION_ID Solution ID
-- # SPTNAC Accelerometer
-- # SPTNMISC Miscellaneous Movement Sensor
-- # SPTNRO Rotation Monitor
-- # SPTNVI Vibration Monitor
-- # SSUDBA Basin
-- # SSUDBP Balancing Pond
-- # SSUDGC SUD Geocellular
-- # SSUDSW Swale
-- # STAKFA Flare Stack
-- # STAKFL Flue Stack
-- # STAKMISC Miscellaneous Stacks
-- # STAKVS Vent Stack
-- # STARAT Auto Transformer Starter


-- # STARDO Direct On Line Starter
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

-- # STARDT Star-Delta Starter
-- # STARLQ Liquid Resistance Starter
-- # STARMISC Miscellaneous Motor Starter
-- # STARRE Resistance Starter
-- # STARRV Reversing Starter
-- # STARSS Soft Starter
-- # STARVF Variable Frequency Starter
-- # STRNBF Strainer with Bag Filter
-- # STRNER Strainer
-- # STRNMISC Miscellaneous Strainer
-- # SWPANL Switch Panel
-- # TANKCC Clean Water Compartment
-- # TANKMISC Miscellaneous Tank
-- # TANKPR Process Tank
-- # TANKST Storage Tank
-- # TBINMISC Miscellaneous Turbine
-- # TBINWA Water Turbine
-- # TBINWI Wind Turbine
-- # THIKBT Belt Thickener
-- # THIKDR Drum Thickener
-- # THIKMISC Miscellaneous Thickening Plant
-- # THIKPF Picket Fence Thickener
-- # THLIIN Threaded Lifting Insert
-- # TNNLIN Tunnel Inlet
-- # TNNLMISC Miscellaneous Tunnel
-- # TNNLOL Tunnel Outlet
-- # TRFMAC Air Cooled Transformer
-- # TRFMLV LV Air Cooled Transformer
-- # TRFMMISC Miscellaneous Transformer
-- # TRFMOC Oil Cooled Transformer
-- # TRFMRT Rectifier Transformer
-- # TRIPBU Tripping Battery Unit
-- # TRUTBG Bevel Gearbox
-- # TRUTCY Cycloidal Gearbox
-- # TRUTHB Helical Bevel Gearbox
-- # TRUTHG Helical Gearbox
-- # TRUTMISC Miscellaneous Transmission Unit
-- # TRUTPG Planetary Gearbox
-- # TRUTSG Spur Gearbox
-- # TRUTVA Variator Gearbox
-- # TRUTWG Worm Gearbox
-- # TSTNTT Temperature Monitoring Device
INSERT OR REPLACE INTO s4_classrep.equiclass_tstntt BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_temperature_instrument t;

