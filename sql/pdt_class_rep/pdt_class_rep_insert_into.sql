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


INSERT INTO pdt_class_rep.equi_master_data BY NAME
WITH cte AS (
    SELECT DISTINCT 
        t.entity_name AS entity_name,
        t.base_name AS base_name,
    FROM pdt_raw_data.pdt_eav t
)
SELECT DISTINCT ON(cte.entity_name) 
    hash(cte.entity_name || cte.base_name) AS equipment_key,
    cte.entity_name AS equi_name,
    cte.base_name AS source_file,
    any_value(CASE WHEN eav.attr_name = 'asset_type' THEN upper(eav.attr_value) ELSE NULL END) AS equipment_type,
    any_value(CASE WHEN eav.attr_name = 'manufacturer' THEN upper(eav.attr_value) ELSE NULL END) AS manufacturer,
    any_value(CASE WHEN eav.attr_name = 'product_range' THEN upper(eav.attr_value) ELSE NULL END) AS model,
    any_value(CASE WHEN eav.attr_name = 'product_model_number' THEN upper(eav.attr_value) ELSE NULL END) AS specific_model_frame,
    any_value(CASE WHEN eav.attr_name = 'manufacturer_s_serial_number' THEN upper(eav.attr_value) ELSE NULL END) AS serial_number,
    any_value(CASE WHEN eav.attr_name = 'asset_status' THEN upper(eav.attr_value) ELSE NULL END) AS asset_status,
    any_value(CASE WHEN eav.attr_name = 'tag_reference' THEN upper(eav.attr_value) ELSE NULL END) AS p_and_i_tag,
    any_value(CASE WHEN eav.attr_name = 'date_of_installation' THEN upper(eav.attr_value) ELSE NULL END) AS installed_from,
    any_value(CASE WHEN eav.attr_name = 'weight_kg' THEN eav.attr_value ELSE NULL END) AS weight_kg,
FROM cte
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = cte.entity_name AND eav.base_name = cte.base_name
GROUP BY cte.entity_name, cte.base_name;

-- # EQUICLASSES

-- ## ACTUEM ()

INSERT INTO pdt_class_rep.equiclass_actuem BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'ip_rating' THEN eav.attr_value ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attr_name = 'rated_current_a' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS actu_rated_current_a,
    any_value(CASE WHEN eav.attr_name = 'rated_power_kw' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS actu_rated_power_kw,
    any_value(CASE WHEN eav.attr_name = 'number_of_phase' THEN eav.attr_value ELSE NULL END) AS actu_number_of_phase,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS actu_rated_voltage,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage_units' THEN eav.attr_value ELSE NULL END) AS actu_rated_voltage_units,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'ELECTRIC MOTOR ACTUATOR'
GROUP BY e.source_file, e.equi_name;


-- ## CONPNL ()

INSERT INTO pdt_class_rep.equiclass_conpnl BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS conp_rated_voltage,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage_units' THEN eav.attr_value ELSE NULL END) AS conp_rated_voltage_units,
    any_value(CASE WHEN eav.attr_name = 'single_line_drawing_ref_number' THEN eav.attr_value ELSE NULL END) AS conp_sld_ref_no,
    any_value(CASE WHEN eav.attr_name = 'number_of_phase' THEN eav.attr_value ELSE NULL END) AS conp_number_of_phase,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'CONTROL PANEL'
GROUP BY e.source_file, e.equi_name;

-- ## DECOEB (Emergency Eye Wash Station)

INSERT INTO pdt_class_rep.equiclass_decoeb BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'EMERGENCY EYE WASH STATION'
GROUP BY e.source_file, e.equi_name;

-- ## DECOES (Emergency Eye Wash and Shower)

INSERT INTO pdt_class_rep.equiclass_decoes BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'EMERGENCY EYE WASH AND SHOWER'
GROUP BY e.source_file, e.equi_name;

-- ## DISTBD ()

INSERT INTO pdt_class_rep.equiclass_distbd BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'ip_rating' THEN eav.attr_value ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attr_name = 'fault_rating_ka' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS dist_fault_rating_ka,
    any_value(CASE WHEN eav.attr_name = 'number_of_phase' THEN eav.attr_value ELSE NULL END) AS dist_number_of_phase,
    any_value(CASE WHEN eav.attr_name = 'number_of_poles' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS dist_number_of_poles,
    any_value(CASE WHEN eav.attr_name = 'number_of_ways' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS dist_number_of_ways,
    any_value(CASE WHEN eav.attr_name = 'rated_current_a' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS dist_rated_current_a,
    any_value(CASE WHEN eav.attr_name = 'single_line_drawing_ref_number' THEN eav.attr_value ELSE NULL END) AS dist_sld_ref_no,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS dist_rated_voltage,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage_units' THEN eav.attr_value ELSE NULL END) AS dist_rated_voltage_units,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'DISTRIBUTION BOARD'
GROUP BY e.source_file, e.equi_name;

-- ## EMTRIN ()

INSERT INTO pdt_class_rep.equiclass_emtrin BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'rated_current_a' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS emtr_rated_current_a,
    any_value(CASE WHEN eav.attr_name = 'rated_power_kw' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS emtr_rated_power_kw,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS emtr_rated_voltage,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage_units' THEN eav.attr_value ELSE NULL END) AS emtr_rated_voltage_units,
    any_value(CASE WHEN eav.attr_name = 'rated_speed_rpm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS emtr_rated_speed_rpm,
    any_value(CASE WHEN eav.attr_name = 'insulation_class_c' THEN eav.attr_value ELSE NULL END) AS insulation_class_deg_c
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'INDUCTION MOTOR'
GROUP BY e.source_file, e.equi_name;

-- ## GASWMG (Magnetic Interlock Position Switch)

INSERT INTO pdt_class_rep.equiclass_gaswmg BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'ip_rating' THEN eav.attr_value ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attr_name = 'output_signal_type' THEN eav.attr_value ELSE NULL END) AS gasw_signal_type,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS gasw_rated_voltage,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage_units' THEN eav.attr_value ELSE NULL END) AS gasw_rated_voltage_units,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'MAGNETIC INTERLOCK POSITION SWITCH'
GROUP BY e.source_file, e.equi_name;

-- ## HEATIM (Immersion Heater)

INSERT INTO pdt_class_rep.equiclass_heatim BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'rated_current_a' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS heat_rated_current_a,
    any_value(CASE WHEN eav.attr_name = 'rated_power_kw' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS heat_rated_power_kw,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS heat_rated_voltage,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage_units' THEN eav.attr_value ELSE NULL END) AS heat_rated_voltage_units,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'IMMERSION HEATER'
GROUP BY e.source_file, e.equi_name;

-- ## INTFLO (Local Operator Interface for PLCs)

INSERT INTO pdt_class_rep.equiclass_intflo BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'ip_rating' THEN eav.attr_value ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS intf_rated_voltage,
    any_value(CASE WHEN eav.attr_name = 'rated_voltage_units' THEN eav.attr_value ELSE NULL END) AS intf_rated_voltage_units,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'LOCAL OPERATOR INTERFACE FOR PLCS'
GROUP BY e.source_file, e.equi_name;

-- ## KISKKI ()

INSERT INTO pdt_class_rep.equiclass_kiskki BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'cat_flap_available' THEN upper(eav.attr_value) ELSE NULL END) AS kisk_cat_flap_available,
    any_value(CASE WHEN eav.attr_name = 'height_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS kisk_height_mm,
    any_value(CASE WHEN eav.attr_name = 'width_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS kisk_width_mm,
    any_value(CASE WHEN eav.attr_name = 'depth_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS kisk_depth_mm,
    any_value(CASE WHEN eav.attr_name = 'material' THEN upper(eav.attr_value) ELSE NULL END) AS kisk_material,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'KIOSK'
GROUP BY e.source_file, e.equi_name;

-- ## LIDEEM (Emergency Lighting)

INSERT INTO pdt_class_rep.equiclass_lideem BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'EMERGENCY LIGHTING'
GROUP BY e.source_file, e.equi_name;

-- ## LIDEEX (Exterior Lighting)

INSERT INTO pdt_class_rep.equiclass_lideex BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'EXTERIOR LIGHTING'
GROUP BY e.source_file, e.equi_name;

-- ## LIDEIN (Interior Lighting)

INSERT INTO pdt_class_rep.equiclass_lidein BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'INTERIOR LIGHTING'
GROUP BY e.source_file, e.equi_name;


-- ## LSTNCO ()

INSERT INTO pdt_class_rep.equiclass_lstnco BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'CONDUCTIVE LEVEL DEVICE'
GROUP BY e.source_file, e.equi_name;

-- ## LSTNFL ()

INSERT INTO pdt_class_rep.equiclass_lstnfl BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'supply_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS lstn_supply_voltage,
    any_value(CASE WHEN eav.attr_name = 'supply_voltage_units' THEN eav.attr_value ELSE NULL END) AS lstn_supply_voltage_units,
    any_value(CASE WHEN eav.attr_name = 'output_signal_type' THEN eav.attr_value ELSE NULL END) AS lstn_signal_type,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'LEVEL FLOAT DEVICE'
GROUP BY e.source_file, e.equi_name;

-- ## LSTNUT ()

INSERT INTO pdt_class_rep.equiclass_lstnut BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'ip_rating' THEN eav.attr_value ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attr_name = 'range_max' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS lstn_range_max,
    any_value(CASE WHEN eav.attr_name = 'range_min' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS lstn_range_min,
    any_value(CASE WHEN eav.attr_name = 'range_unit' THEN eav.attr_value ELSE NULL END) AS lstn_range_units,
    any_value(CASE WHEN eav.attr_name = 'output_signal_type' THEN eav.attr_value ELSE NULL END) AS lstn_signal_type,
    any_value(CASE WHEN eav.attr_name = 'supply_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS lstn_supply_voltage,
    any_value(CASE WHEN eav.attr_name = 'supply_voltage_units' THEN eav.attr_value ELSE NULL END) AS lstn_supply_voltage_units,
    any_value(CASE WHEN eav.attr_name = 'transducer_model' THEN eav.attr_value ELSE NULL END) AS lstn_transducer_model,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'ULTRASONIC TIME OF FLIGHT LEVEL DEVICE'
GROUP BY e.source_file, e.equi_name;

-- ## NETWMO (Network Device connecting to Telecoms)

INSERT INTO pdt_class_rep.equiclass_netwmo BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'supply_voltage' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS netw_supply_voltage,
    any_value(CASE WHEN eav.attr_name = 'supply_voltage_units' THEN eav.attr_value ELSE NULL END) AS netw_supply_voltage_units,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'NETWORK DEVICE CONNECTING TO TELECOMS'
GROUP BY e.source_file, e.equi_name;

-- ## PSTNDI ()

INSERT INTO pdt_class_rep.equiclass_pstndi BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'range_max' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pstn_range_max,
    any_value(CASE WHEN eav.attr_name = 'range_min' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pstn_range_min,
    any_value(CASE WHEN eav.attr_name = 'range_unit' THEN eav.attr_value ELSE NULL END) AS pstn_range_units,
    any_value(CASE WHEN eav.attr_name = 'output_signal_type' THEN eav.attr_value ELSE NULL END) AS pstn_signal_type,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'DIAPHRAGM TYPE PRESSURE DEVICE'
GROUP BY e.source_file, e.equi_name;

-- ## PUMPCE (Centrifugal Pump)
-- TODO unlikely we will need CTEs and calculated columns, keeping it here for a CTE example... 
INSERT INTO pdt_class_rep.equiclass_pumpce BY NAME 
WITH cte AS(
    SELECT DISTINCT ON(e.equi_name, e.source_file)   
        hash(e.equi_name || e.source_file) AS equipment_key,
        any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
        any_value(CASE WHEN eav.attr_name = 'flow_l_s' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pump_flow_litres_per_sec,
        any_value(CASE WHEN eav.attr_name = 'rated_power_kw' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pump_rated_power_kw,
        any_value(CASE WHEN eav.attr_name = 'inlet_size_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS pump_inlet_size_mm,
        any_value(CASE WHEN eav.attr_name = 'outlet_size_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS pump_outlet_size_mm,
        any_value(CASE WHEN eav.attr_name = 'installed_design_head_m' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pump_installed_design_head_m,
    FROM pdt_class_rep.equi_master_data e
    JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
    WHERE e.equipment_type = 'CENTRIFUGAL PUMP'
    GROUP BY e.source_file, e.equi_name)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## PUMPDI ()

INSERT INTO pdt_class_rep.equiclass_pumpdi BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'ip_rating' THEN eav.attr_value ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attr_name = 'flow_l_s' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pump_flow_litres_per_sec,
    any_value(CASE WHEN eav.attr_name = 'rated_power_kw' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pump_rated_power_kw,
    any_value(CASE WHEN eav.attr_name = 'inlet_size_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS pump_inlet_size_mm,
    any_value(CASE WHEN eav.attr_name = 'outlet_size_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS pump_outlet_size_mm,
    any_value(CASE WHEN eav.attr_name = 'pumped_medium' THEN eav.attr_value ELSE NULL END) AS pump_media_type,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'DIAPHRAGM PUMP'
GROUP BY e.source_file, e.equi_name;

-- ## VALVBA ()

INSERT INTO pdt_class_rep.equiclass_valvba BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'inlet_size_mm' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS valv_inlet_size_mm,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'BALL VALVE'
GROUP BY e.source_file, e.equi_name;

-- ## VALVGA ()

INSERT INTO pdt_class_rep.equiclass_valvga BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'inlet_size_mm' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS valv_inlet_size_mm,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'GATE VALVE'
GROUP BY e.source_file, e.equi_name;

-- ## VALVNR ()

INSERT INTO pdt_class_rep.equiclass_valvnr BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'inlet_size_mm' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS valv_inlet_size_mm,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'NON RETURN VALVE'
GROUP BY e.source_file, e.equi_name;


-- ## VALVPR (Pressure Reducing Valve)

INSERT INTO pdt_class_rep.equiclass_valvpr BY NAME 
SELECT DISTINCT ON(e.equi_name, e.source_file)   
    hash(e.equi_name || e.source_file) AS equipment_key,
    any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN upper(eav.attr_value) ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attr_name = 'memo_line' THEN eav.attr_value ELSE NULL END) AS memo_line,
    any_value(CASE WHEN eav.attr_name = 'inlet_size_mm' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS valv_inlet_size_mm,
FROM pdt_class_rep.equi_master_data e
JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
WHERE e.equipment_type = 'PRESSURE REDUCING VALVE'
GROUP BY e.source_file, e.equi_name;





