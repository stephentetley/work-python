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


-- ## PUMPCE (Centrifugal Pump)

INSERT INTO pdt_class_rep.equiclass_pumpce BY NAME 
WITH cte AS(
    SELECT DISTINCT ON(e.equi_name, e.source_file)   
        e.equi_name AS equi_name,
        e.source_file AS source_file,
        any_value(CASE WHEN eav.attr_name = 'location_on_site' THEN eav.attr_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attr_name = 'flow_l_s' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pump_flow_litres_per_sec,
        any_value(CASE WHEN eav.attr_name = 'rated_power_kw' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pump_rated_power_kw,
        any_value(CASE WHEN eav.attr_name = 'inlet_size_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS pump_inlet_size_mm,
        any_value(CASE WHEN eav.attr_name = 'outlet_size_mm' THEN TRY_CAST(eav.attr_value AS INTEGER) ELSE NULL END) AS pump_outlet_size_mm,
        any_value(CASE WHEN eav.attr_name = 'installed_design_head_m' THEN TRY_CAST(eav.attr_value AS DECIMAL) ELSE NULL END) AS pump_installed_design_head_m,
    FROM pdt_class_rep.equi_master_data e
    JOIN pdt_raw_data.pdt_eav eav ON eav.entity_name = e.equi_name AND eav.base_name = e.source_file
    WHERE e.equipment_type LIKE 'CENTRIFUGAL PUMP'
    GROUP BY e.source_file, e.equi_name)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;
