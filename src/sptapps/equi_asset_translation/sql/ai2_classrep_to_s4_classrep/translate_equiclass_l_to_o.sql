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

-- # LIACBC Beam Clamp
-- # LIACBR Blue Rope Assembly
-- # LIACBS Bow Shackle
-- # LIACDS D Shackle
-- # LIACEB Eye Bolt
-- # LIACHL Lifting Hook
-- # LIACLB Lifting Bracket
-- # LIACLF Lifting Frame
-- # LIACLT Lifting Tackle
-- # LIACMISC Miscellaneous Lifting Accessories
-- # LIACPL Plate Lift Clamp
-- # LIACTW Threaded Wire Rope Loop
-- # LICHPU Pump Lifting Chain
-- # LIDEEM Emergency Lighting
-- # LIDEEX Exterior Lighting
-- # LIDEHM High Mast Lighting Column
-- # LIDEIN Interior Lighting
-- # LIDEMISC Miscellaneous Lighting Device
-- # LIFRHY Hydraulic Lifter
-- # LIFRMISC Miscellaneous Lifter
-- # LIFTPA Passenger Lift
-- # LIPOCO Lightning Conductor
-- # LISLBS Belt Sling
-- # LISLCS Chain Sling
-- # LISLMISC Miscellaneous Lifting Sling
-- # LISLRS Round Sling
-- # LISLWR Lifting Wire Rope
-- # LLAPAP Anchor Point
-- # LLBCBC Beam Clamp
-- # LLBFLI Lifting Beams / Frames
-- # LLBFSP Spreader Beam
-- # LLCCFL Floor Crane
-- # LLCCGA Gantry Crane
-- # LLCCGO Goliath Crane
-- # LLCCOE Overhead Crane - electric
-- # LLCCOM Overhead Crane - manual
-- # LLCSCS Chain Sling
-- # LLDDDA Davit Jib
-- # LLDDEP Extension Post
-- # LLDSDS Davit Socket
-- # LLEBBO Eye Bolt
-- # LLEBNU Eye Nut
-- # LLFSRP Ropes (of Ropes / Slings)
-- # LLGGAF A-frame
-- # LLGGFX Fixed Gantry
-- # LLGGPT Portable Gantry
-- # LLJCJI Jib Crane
-- # LLJCPI Pillar Crane
-- # LLJCPJ Pillar Jib Crane
-- # LLJCSW Swing Jib Crane
-- # LLJCWA Wall Jib Crane
-- # LLJJCK Jack
-- # LLJJHM Hydraulic Manhole Lifters
-- # LLLCLO Load Cell
-- # LLLHHO Hooks
-- # LLLSPU Passenger / Utility Lift
-- # LLMHCH Chain Hoist
-- # LLMHCO Combined Hoist
-- # LLMHEH Engine Hoist
-- # LLMHLE Lever Block / Lever Hoist / Pull Lift
-- # LLMHRP Rope Hoist
-- # LLMLMA Magnet
-- # LLMLMH Manhole Lifter
-- # LLPCSS Stainless Steel Sub Pump Lifting Chain
-- # LLPHEL Electric Hoist
-- # LLPLCL Clamp
-- # LLPLPL Plate
-- # LLPTPA Pallet Truck
-- # LLRRSP Specialist Beams
-- # LLRRTB Runway Tracks and Beams
-- # LLRTRI Rigging Screw
-- # LLRTTU Turnbuckle
-- # LLSBGI Gin Wheel
-- # LLSBSH Sheave Block
-- # LLSSAB Safety Pin Bow Shackle
-- # LLSSAD Safety Pin Dee shackle
-- # LLSSCB Screw Pin Bow Shackle
-- # LLSSCD Screw Pin Dee shackle
-- # LLTTTR Trolley
-- # LLUABL Bolt On Lug
-- # LLUABR Bracket
-- # LLUACA Carrier
-- # LLUACO Collar
-- # LLUAGI Girder Clip
-- # LLUALI Link
-- # LLUEFA Fork Lift Attachment
-- # LLUEFT Fork Lift Truck
-- # LLUEHI Hiab
-- # LLUESC Scissor Lift
-- # LLUETA Telehandler Attachment
-- # LLUETL Telescopic Load Ladder
-- # LLWAAB Fall Arrest Block
-- # LLWAAL Anchor Line
-- # LLWAAW Fall Arrest Winch
-- # LLWABC Bosins Chair
-- # LLWAIN Inertia Reel
-- # LLWAKA Karabiner
-- # LLWALY Lanyard
-- # LLWAMA Man Riding Winch
-- # LLWARK Rescue Kit
-- # LLWASH Safety Harness
-- # LLWASK Shock Absorber
-- # LLWASL Safety Line
-- # LLWATR Tripod
-- # LLWRWI Wire Rope Slings
-- # LLWSPS Polyester Round Slings
-- # LLWSWS Webbing Slings (belt)
-- # LLWWLO Load Lifting Winch
    
-- # LSTNUT Ultrasonic Time of Flight Level Device
    
-- LSTNCO
INSERT OR REPLACE INTO s4_classrep.equiclass_lstnco BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    udf_format_output_type(t._signal_unit) AS lstn_output_type,
    t._range_min AS lstn_range_min,
    t._range_max AS lstn_range_max,
    upper(t._range_unit) AS lstn_range_units,
    udf_format_signal3(t._signal_min, t._signal_max, t._signal_unit) AS lstn_signal_type,
FROM ai2_classrep.equiclass_conductivity_level_instrument t;

-- # LSTNCP Capacitive Level Device
-- # LSTNFL Level Float Device
-- # LSTNME Mechanical Level Device
-- # LSTNMG Magnetic Level Device
-- # LSTNMISC Miscellaneous Level Transmitter
-- # LSTNNU Radioactive Source Level Device
-- # LSTNOP Optical Light Level Device
-- # LSTNPR Pressure converted for Level Device
-- # LSTNPU Ultrasonic Doppler Device
-- # LSTNRD Radar Level Device
-- # LSTNTF Vibrating Tuning Fork Level Device
-- # LSTNTP Tipping Bucket Type Rain Gauge Device
-- # LSTNUS Sludge Blanket Level Device

-- LSTNUT
INSERT OR REPLACE INTO s4_classrep.equiclass_lstnut BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    -- relay 1
    t._relay_1_function AS lstn_relay_1_function,
    TRY_CAST(t._relay_1_off_level_m AS DECIMAL) AS lstn_relay_1_off_level_m,
    TRY_CAST(t._relay_1_on_level_m AS DECIMAL) AS lstn_relay_1_on_level_m,
    -- relay 2
    t._relay_2_function AS lstn_relay_2_function,
    TRY_CAST(t._relay_2_off_level_m AS DECIMAL) AS lstn_relay_2_off_level_m,
    TRY_CAST(t._relay_2_on_level_m AS DECIMAL) AS lstn_relay_2_on_level_m,
    -- relay 3
    t._relay_3_function AS lstn_relay_3_function,
    TRY_CAST(t._relay_3_off_level_m AS DECIMAL) AS lstn_relay_3_off_level_m,
    TRY_CAST(t._relay_3_on_level_m AS DECIMAL) AS lstn_relay_3_on_level_m,
    -- relay 4
    t._relay_4_function AS lstn_relay_4_function,
    TRY_CAST(t._relay_4_off_level_m AS DECIMAL) AS lstn_relay_4_off_level_m,
    TRY_CAST(t._relay_4_on_level_m AS DECIMAL) AS lstn_relay_4_on_level_m,
    -- relay 5
    t._relay_5_function AS lstn_relay_5_function,
    TRY_CAST(t._relay_5_off_level_m AS DECIMAL) AS lstn_relay_5_off_level_m,
    TRY_CAST(t._relay_5_on_level_m AS DECIMAL) AS lstn_relay_5_on_level_m,
    -- relay 6
    t._relay_6_function AS lstn_relay_6_function,
    TRY_CAST(t._relay_6_off_level_m AS DECIMAL) AS lstn_relay_6_off_level_m,
    TRY_CAST(t._relay_6_on_level_m AS DECIMAL) AS lstn_relay_6_on_level_m,
    
    t._transducer_type AS lstn_transducer_model,
    t._transducer_serial_no AS lstn_transducer_serial_no,
    t._range_min AS lstn_range_min,
    t._range_max AS lstn_range_max,
    upper(t._range_unit) AS lstn_range_units,
    udf_format_signal3(t._signal_min, t._signal_max, t._signal_unit) AS lstn_signal_type,
FROM ai2_classrep.equiclass_ultrasonic_level_instrument t;

-- # MACTOR Macerator
-- # MAGNCD Magnetic Capture Device
-- # MCCEPA Motor Control Centre Panel
-- # METREL Electricity Meter
-- # MIXRMISC Miscellaneous Mixer
-- # MIXRRO Rotary Mixer
-- # MIXRST Static Mixer
-- # MIXRSU Submersible Mixer
-- # MOLPAP Access Platform
-- # MOLPMISC Miscellaneous Mobile Lifting Plant
-- # MOLPPT Pallet Truck
-- # MOLPSL Scissor Lift Platform
-- # MOSWHM Humidity measuring Moisture Switch
-- # NAVBBY Navigation Buoy

-- # NETWCM Condition Monitoring Comms Network (no ai2 match)

-- # NETWCO Network Node converting Signals (no ai2 match)

-- # NETWEN Ethernet Network Hub
INSERT OR REPLACE INTO s4_classrep.equiclass_netwen BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ethernet t;

-- # NETWMB MODBUS Network Hub (no ai2 match)

-- # NETWMO Modem
INSERT OR REPLACE INTO s4_classrep.equiclass_netwmo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_modem t;


-- # NETWPB PROFIBUS Network Hub
INSERT OR REPLACE INTO s4_classrep.equiclass_netwpb BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_profibus t;


-- # NETWRA Network Device connecting via Radio (two)
INSERT OR REPLACE INTO s4_classrep.equiclass_netwra BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_misc_radio_com_equipment t;

INSERT OR REPLACE INTO s4_classrep.equiclass_netwra BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_radio_tx_rx_control_equipment t;

-- NETWTL
INSERT OR REPLACE INTO s4_classrep.equiclass_netwtl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_telemetry_outstation t;

-- # ODCOIO Ionisation Unit
-- # ODSYAD Odour Adsorber
-- # ODSYFI Odour Filter
-- # ODSYMISC Miscellaneous Odour Filter
-- # ODSYSC Odour Scrubber
-- # OVFLMISC Miscellaneous Overflow
-- # OVFLSH Overflow Shaft
-- # OVFLSP Spillway Channel Overflow
-- # OVFLWE Overflow