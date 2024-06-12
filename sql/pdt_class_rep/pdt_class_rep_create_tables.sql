-- 
-- Copyright 2024 Stephen Tetley
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

CREATE SCHEMA IF NOT EXISTS pdt_class_rep;

CREATE OR REPLACE TABLE pdt_class_rep.equi_master_data (
    equi_name VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    equipment_type VARCHAR,
    installed_from VARCHAR,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    asset_status VARCHAR,
    p_and_i_tag VARCHAR,
    weight_kg VARCHAR,
);


CREATE OR REPLACE TABLE pdt_class_rep.equiclass_pumpce (
    equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL,
    location_on_site VARCHAR,
    manufacturers_asset_life_yr INTEGER,
    memo_line VARCHAR,
    pump_flow_litres_per_sec DECIMAL(8, 2),
    pump_impeller_type VARCHAR,
    pump_inlet_size_mm INTEGER,
    pump_installed_design_head_m DECIMAL(7, 2),
    pump_media_type VARCHAR,
    pump_number_of_stage INTEGER,
    pump_outlet_size_mm INTEGER,
    pump_rated_power_kw DECIMAL(7, 3),
    pump_rated_speed_rpm INTEGER,
    uniclass_code VARCHAR,
    uniclass_desc VARCHAR,
);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_lstnut(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, 
    ip_rating VARCHAR, location_on_site VARCHAR, lstn_range_max DECIMAL(9, 3), lstn_range_min DECIMAL(9, 3), lstn_range_units VARCHAR, lstn_relay_1_function VARCHAR, lstn_relay_1_off_level_m DECIMAL(7, 3), lstn_relay_1_on_level_m DECIMAL(8, 3), lstn_relay_2_function VARCHAR, lstn_relay_2_off_level_m DECIMAL(7, 3), lstn_relay_2_on_level_m DECIMAL(8, 3), lstn_relay_3_function VARCHAR, lstn_relay_3_off_level_m DECIMAL(7, 3), lstn_relay_3_on_level_m DECIMAL(8, 3), lstn_relay_4_function VARCHAR, lstn_relay_4_off_level_m DECIMAL(6, 3), lstn_relay_4_on_level_m DECIMAL(6, 3), lstn_relay_5_function VARCHAR, lstn_relay_5_off_level_m DECIMAL(6, 3), lstn_relay_5_on_level_m DECIMAL(6, 3), lstn_relay_6_function VARCHAR, lstn_relay_6_off_level_m DECIMAL(5, 3), lstn_relay_6_on_level_m DECIMAL(5, 3), lstn_set_to_snort VARCHAR, lstn_signal_type VARCHAR, lstn_supply_voltage INTEGER, lstn_supply_voltage_units VARCHAR, lstn_transducer_model VARCHAR, lstn_transducer_serial_no VARCHAR, lstn_transmitter_model VARCHAR, lstn_transmitter_serial_no VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_valvba(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, 
    location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR, valv_inlet_size_mm INTEGER, valv_media_type VARCHAR, valv_valve_configuration VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_lstnco(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, ip_rating VARCHAR, location_on_site VARCHAR, lstn_output_type VARCHAR, lstn_range_max DECIMAL(9, 3), lstn_range_min DECIMAL(9, 3), lstn_range_units VARCHAR, lstn_signal_type VARCHAR, lstn_supply_voltage INTEGER, lstn_supply_voltage_units VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_conpnl(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, conp_number_of_phase VARCHAR, conp_number_of_ways INTEGER, conp_rated_current_a DECIMAL(7, 2), conp_rated_voltage INTEGER, conp_rated_voltage_units VARCHAR, conp_sld_ref_no VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_pumpdi(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, insulation_class_deg_c VARCHAR, ip_rating VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, pump_diaphragm_material VARCHAR, pump_flow_litres_per_sec DECIMAL(8, 2), pump_inlet_size_mm INTEGER, pump_installed_design_head_m DECIMAL(7, 2), pump_media_type VARCHAR, pump_motor_type VARCHAR, pump_outlet_size_mm INTEGER, pump_rated_power_kw DECIMAL(7, 3), pump_rated_speed_rpm INTEGER, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_distbd(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, dist_fault_rating_ka DECIMAL(6, 2), dist_number_of_phase VARCHAR, dist_number_of_poles INTEGER, dist_number_of_ways INTEGER, dist_rated_current_a DECIMAL(7, 2), dist_rated_voltage INTEGER, dist_rated_voltage_units VARCHAR, dist_sld_ref_no VARCHAR, ip_rating VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_actuem(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, actu_atex_code VARCHAR, actu_number_of_phase VARCHAR, actu_rated_current_a DECIMAL(7, 2), actu_rated_power_kw DECIMAL(6, 3), actu_rated_voltage INTEGER, actu_rated_voltage_units VARCHAR, actu_speed_rpm DECIMAL(7, 3), actu_valve_torque_nm DECIMAL(5, 1), insulation_class_deg_c VARCHAR, ip_rating VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_decoes(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_decoeb(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_lideem(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, ip_rating VARCHAR, lide_automatic_self_test VARCHAR, lide_battery_size_ah INTEGER, lide_power_supply_type VARCHAR, lide_rated_voltage INTEGER, lide_rated_voltage_units VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_lideex(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, ip_rating VARCHAR, lide_rated_voltage INTEGER, lide_rated_voltage_units VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_lidein(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_valvga(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR, valv_inlet_size_mm INTEGER);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_heatim(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, heat_rated_current_a DECIMAL(6, 2), heat_rated_power_kw DECIMAL(7, 2), heat_rated_voltage INTEGER, heat_rated_voltage_units VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_emtrin(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, emtr_anti_condensation_heaters VARCHAR, emtr_atex_code VARCHAR, emtr_frame_size VARCHAR, emtr_mounting_type VARCHAR, emtr_number_of_phase VARCHAR, emtr_rated_current_a DECIMAL(7, 2), emtr_rated_power_kw DECIMAL(7, 3), emtr_rated_speed_rpm INTEGER, emtr_rated_voltage INTEGER, emtr_rated_voltage_units VARCHAR, emtr_thermal_protection VARCHAR, insulation_class_deg_c VARCHAR, ip_rating VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_kiskki(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, kisk_base_height_mm INTEGER, kisk_cat_flap_available VARCHAR, kisk_depth_mm INTEGER, kisk_height_mm INTEGER, kisk_material VARCHAR, kisk_width_mm INTEGER, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_lstnfl(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, ip_rating VARCHAR, location_on_site VARCHAR, lstn_range_max DECIMAL(9, 3), lstn_range_min DECIMAL(9, 3), lstn_range_units VARCHAR, lstn_signal_type VARCHAR, lstn_supply_voltage INTEGER, lstn_supply_voltage_units VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_intflo(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, intf_instrument_power_w DECIMAL(6, 3), intf_rated_voltage INTEGER, intf_rated_voltage_units VARCHAR, ip_rating VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_gaswmg(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, gasw_rated_voltage INTEGER, gasw_rated_voltage_units VARCHAR, gasw_signal_type VARCHAR, ip_rating VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_netwmo(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, ip_rating VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, netw_supply_voltage INTEGER, netw_supply_voltage_units VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_valvnr(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR, valv_flow_litres_per_sec DECIMAL(7, 2), valv_inlet_size_mm INTEGER, valv_rated_temperature_deg_c INTEGER);

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_valvpr(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR, valv_inlet_size_mm INTEGER, valv_rated_pressure_bar DECIMAL(7, 2));

CREATE OR REPLACE TABLE pdt_class_rep.equiclass_pstndi(equi_name VARCHAR NOT NULL, source_file VARCHAR NOT NULL, ip_rating VARCHAR, location_on_site VARCHAR, manufacturers_asset_life_yr INTEGER, memo_line VARCHAR, pstn_pressure_instrument_type VARCHAR, pstn_range_max DECIMAL(10, 3), pstn_range_min DECIMAL(9, 3), pstn_range_units VARCHAR, pstn_signal_type VARCHAR, pstn_supply_voltage INTEGER, pstn_supply_voltage_units VARCHAR, uniclass_code VARCHAR, uniclass_desc VARCHAR);






