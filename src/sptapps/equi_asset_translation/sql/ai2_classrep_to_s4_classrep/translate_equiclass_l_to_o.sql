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
INSERT OR REPLACE INTO s4_classrep.equiclass_liacbc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_beam_clamp t;

-- # LIACBR Blue Rope Assembly
-- # LIACBS Bow Shackle
INSERT OR REPLACE INTO s4_classrep.equiclass_liacbs BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_bow_shackles t;

-- # LIACDS D Shackle
INSERT OR REPLACE INTO s4_classrep.equiclass_liacds BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_dee_shackles t;

-- # LIACEB Eye Bolt
INSERT OR REPLACE INTO s4_classrep.equiclass_liaceb BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_eye_bolts t;

-- # LIACHL Lifting Hook
INSERT OR REPLACE INTO s4_classrep.equiclass_liachl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_lifting_hooks t;


-- # LIACLB Lifting Bracket
-- # LIACLF Lifting Frame
-- # LIACLT Lifting Tackle
INSERT OR REPLACE INTO s4_classrep.equiclass_liaclt BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_unclassified_lifting_tackle t;

-- # LIACMISC Miscellaneous Lifting Accessories

-- # LIACPL Plate Lift Clamp
INSERT OR REPLACE INTO s4_classrep.equiclass_liacpl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_plate_lift_clamps t;

-- # LIACTW Threaded Wire Rope Loop
-- # LICHPU Pump Lifting Chain
INSERT OR REPLACE INTO s4_classrep.equiclass_lichpu BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_pump_lifting_chain t;

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
INSERT OR REPLACE INTO s4_classrep.equiclass_lislbs BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_belt_slings t;

-- # LISLCS Chain Sling
INSERT OR REPLACE INTO s4_classrep.equiclass_lislcs BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_chain_slings t;

-- # LISLMISC Miscellaneous Lifting Sling
-- # LISLRS Round Sling
-- # LISLWR Lifting Wire Rope
-- # LLAPAP Anchor Point
INSERT OR REPLACE INTO s4_classrep.equiclass_llapap BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_anchor_point t;

-- # LLBCBC Beam Clamp
INSERT OR REPLACE INTO s4_classrep.equiclass_llbcbc BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_beam_clamp t;

-- # LLBFLI Lifting Beams / Frames
-- # LLBFSP Spreader Beam
-- # LLCCFL Floor Crane
-- # LLCCGA Gantry Crane
-- # LLCCGO Goliath Crane
-- # LLCCOE Overhead Crane - electric
-- # LLCCOM Overhead Crane - manual
-- # LLCSCS Chain Sling
-- # LLDDDA Davit Jib
INSERT OR REPLACE INTO s4_classrep.equiclass_llddda BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_davit t;

-- # LLDDEP Extension Post
-- # LLDSDS Davit Socket
INSERT OR REPLACE INTO s4_classrep.equiclass_lldsds BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_davit_sockets t;

-- # LLEBBO Eye Bolt
INSERT OR REPLACE INTO s4_classrep.equiclass_llebbo BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_eye_bolts t;

-- # LLEBNU Eye Nut
-- # LLFSRP Ropes (of Ropes / Slings)
-- # LLGGAF A-frame
-- # LLGGFX Fixed Gantry
-- # LLGGPT Portable Gantry
-- # LLJCJI Jib Crane
INSERT OR REPLACE INTO s4_classrep.equiclass_lljcji BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_jib_crane t;

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
INSERT OR REPLACE INTO s4_classrep.equiclass_llsbsh BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_sheave_blocks t;

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
    udfx.format_output_type(t._signal_unit) AS lstn_output_type,
    t._range_min AS lstn_range_min,
    t._range_max AS lstn_range_max,
    upper(t._range_unit) AS lstn_range_units,
    udfx.format_signal3(t._signal_min, t._signal_max, t._signal_unit) AS lstn_signal_type,
FROM ai2_classrep.equiclass_conductivity_level_instrument t;

-- # LSTNCP Capacitive Level Device
INSERT OR REPLACE INTO s4_classrep.equiclass_lstncp BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_capacitance_level_instrument t;

-- # LSTNFL Level Float Device
INSERT OR REPLACE INTO s4_classrep.equiclass_lstnfl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_float_level_instrument t;

-- # LSTNME Mechanical Level Device
-- # LSTNMG Magnetic Level Device
-- # LSTNMISC Miscellaneous Level Transmitter
-- # LSTNNU Radioactive Source Level Device

-- # LSTNOP Optical Light Level Device
INSERT OR REPLACE INTO s4_classrep.equiclass_lstnop BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    t._range_min AS lstn_range_min,
    t._range_max AS lstn_range_max,
    upper(t._range_unit) AS lstn_range_units,
    udfx.format_signal3(t._signal_min, t._signal_max, t._signal_unit) AS lstn_signal_type,
FROM ai2_classrep.equiclass_optical_level_instrument t;

-- # LSTNPR Pressure converted for Level Device
-- # LSTNPU Ultrasonic Doppler Device

-- # LSTNRD Radar Level Device
INSERT OR REPLACE INTO s4_classrep.equiclass_lstnrd BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
    t._range_min AS lstn_range_min,
    t._range_max AS lstn_range_max,
    upper(t._range_unit) AS lstn_range_units,
    udfx.format_signal3(t._signal_min, t._signal_max, t._signal_unit) AS lstn_signal_type,
FROM ai2_classrep.equiclass_radar_level_instrument t;

-- # LSTNTF Vibrating Tuning Fork Level Device
INSERT OR REPLACE INTO s4_classrep.equiclass_lstntf BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_tuning_fork_level_instrument t;

-- # LSTNTP Tipping Bucket Type Rain Gauge Device

-- # LSTNUS Sludge Blanket Level Device
INSERT OR REPLACE INTO s4_classrep.equiclass_lstnus BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_sludge_blanket_level_instrument t;

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
    udfx.format_signal3(t._signal_min, t._signal_max, t._signal_unit) AS lstn_signal_type,
FROM ai2_classrep.equiclass_ultrasonic_level_instrument t;

-- # MACTOR Macerator
-- # MAGNCD Magnetic Capture Device

-- # MCCEPA Motor Control Centre Panel
INSERT OR REPLACE INTO s4_classrep.equiclass_mccepa BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_mcc_unit t;

-- # METREL Electricity Meter
INSERT OR REPLACE INTO s4_classrep.equiclass_metrel BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_electric_meter t;

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
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_misc_radio_com_equipment t)
UNION
(SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_radio_tx_rx_control_equipment t);

-- NETWTL
INSERT OR REPLACE INTO s4_classrep.equiclass_netwtl BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_telemetry_outstation t;

-- # ODCOIO Ionisation Unit
INSERT OR REPLACE INTO s4_classrep.equiclass_odcoio BY NAME
SELECT
    t.equipment_id AS equipment_id,  
    t._location_on_site AS location_on_site,
FROM ai2_classrep.equiclass_ionization_unit t;

-- # ODSYAD Odour Adsorber
-- # ODSYFI Odour Filter
-- # ODSYMISC Miscellaneous Odour Filter
-- # ODSYSC Odour Scrubber
-- # OVFLMISC Miscellaneous Overflow
-- # OVFLSH Overflow Shaft
-- # OVFLSP Spillway Channel Overflow
-- # OVFLWE Overflow
INSERT OR REPLACE INTO s4_classrep.equiclass_ovflwe BY NAME
SELECT
    t.equipment_id AS equipment_id,  
FROM ai2_classrep.equiclass_overflow t;