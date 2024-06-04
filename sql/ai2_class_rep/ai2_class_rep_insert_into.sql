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



--- source table may have _duplicates_ hence use `DISTINCT ON`...
INSERT INTO ai2_class_rep.equi_master_data BY NAME
SELECT DISTINCT ON (emd.ai2_reference)
    emd.ai2_reference AS ai2_reference,
    emd.common_name AS common_name,
    regexp_extract(emd.common_name, '/([^/]*)/EQUIPMENT:', 1) AS equipment_name,
    regexp_extract(emd.common_name, '.*EQUIPMENT: (.*)$', 1) AS equipment_type,
    emd.installed_from AS installed_from,
    emd.asset_status AS asset_status,
    emd.manufacturer AS manufacturer,
    emd.model AS model,
    any_value(CASE WHEN eav.attribute_name = 'specific_model_frame' THEN eav.attribute_value ELSE NULL END) AS specific_model_frame,
    any_value(CASE WHEN eav.attribute_name = 'serial_no' THEN eav.attribute_value ELSE NULL END) AS serial_number,
    any_value(CASE WHEN eav.attribute_name = 'p_and_i_tag_no' THEN eav.attribute_value ELSE NULL END) AS p_and_i_tag,
    any_value(CASE WHEN eav.attribute_name = 'weight_kg' THEN TRY_CAST(eav.attribute_value AS INTEGER) ELSE NULL END) AS weight_kg,
    any_value(CASE WHEN eav.attribute_name = 'work_centre' THEN eav.attribute_value ELSE NULL END) AS work_centre,
    any_value(CASE WHEN eav.attribute_name = 'responsible_officer' THEN eav.attribute_value ELSE NULL END) AS responsible_officer,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference
WHERE emd.common_name LIKE '%EQUIPMENT:%'
GROUP BY emd.ai2_reference, emd.common_name, emd.installed_from, emd.asset_status, emd.manufacturer, emd.model;


INSERT INTO ai2_class_rep.equi_memo_text BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference,
    any_value(CASE WHEN eav.attribute_name = 'memo_line_1' THEN eav.attribute_value ELSE NULL END) AS memo_line1,
    any_value(CASE WHEN eav.attribute_name = 'memo_line_2' THEN eav.attribute_value ELSE NULL END) AS memo_line2,
    any_value(CASE WHEN eav.attribute_name = 'memo_line_3' THEN eav.attribute_value ELSE NULL END) AS memo_line3,
    any_value(CASE WHEN eav.attribute_name = 'memo_line_4' THEN eav.attribute_value ELSE NULL END) AS memo_line4,
    any_value(CASE WHEN eav.attribute_name = 'memo_line_5' THEN eav.attribute_value ELSE NULL END) AS memo_line5,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT:%'
GROUP BY emd.ai2_reference;


INSERT OR REPLACE INTO ai2_class_rep.equiclass_asset_condition BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'condition_grade' THEN eav.attribute_value ELSE NULL END) AS condition_grade,
    any_value(CASE WHEN eav.attribute_name = 'condition_grade_reason' THEN eav.attribute_value ELSE NULL END) AS condition_grade_reason,
    any_value(CASE WHEN eav.attribute_name = 'agasp_survey_year' THEN TRY_CAST(eav.attribute_value AS INTEGER) ELSE NULL END) AS survey_date,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT:%'
GROUP BY emd.ai2_reference;



-- uses scalar udfs `udf_get_easting` and `udf_get_northing` that must be registered first...
INSERT INTO ai2_class_rep.equiclass_east_north BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN eav.attribute_value ELSE NULL END) AS grid_ref,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN udf_get_easting(eav.attribute_value) ELSE NULL END) AS easting,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN udf_get_northing(eav.attribute_value) ELSE NULL END) AS northing,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT:%'
GROUP BY emd.ai2_reference;

-- ## TEMP TABLES

INSERT INTO temp_power BY NAME
WITH cte AS 
    (SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'power' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS rated_power,
        any_value(CASE WHEN eav.attribute_name = 'power_units' THEN upper(eav.attribute_value) ELSE NULL END) AS power_units,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    GROUP BY emd.ai2_reference)
SELECT 
    t.ai2_reference AS ai2_reference,
    CASE 
        WHEN t.power_units = 'KILOWATTS' THEN t.rated_power
        WHEN t.power_units = 'WATTS' THEN t.rated_power * 1000
        ELSE NULL
    END AS power_kilowatts,
    CASE 
        WHEN t.power_units = 'KILOWATTS' THEN t.rated_power / 1000
        WHEN t.power_units = 'WATTS' THEN t.rated_power
        ELSE NULL
    END AS power_watts,
FROM cte t
WHERE t.power_units IS NOT NULL;


INSERT INTO temp_signal_type BY NAME
WITH cte AS 
    (SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'signal_max' THEN eav.attribute_value ELSE NULL END) AS signal_max,
        any_value(CASE WHEN eav.attribute_name = 'signal_min' THEN eav.attribute_value ELSE NULL END) AS signal_min,
        any_value(CASE WHEN eav.attribute_name = 'signal_unit' THEN eav.attribute_value ELSE NULL END) AS signal_unit,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    GROUP BY emd.ai2_reference)
SELECT 
    t.ai2_reference AS ai2_reference,
    t.signal_min || ' - ' || t.signal_max || ' ' || upper(t.signal_unit) AS signal_type
FROM cte t
WHERE t.signal_unit IS NOT NULL;

INSERT INTO temp_valve_size BY NAME
WITH cte AS 
    (SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'size' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS valve_size,
        any_value(CASE WHEN eav.attribute_name = 'size_units' THEN upper(eav.attribute_value) ELSE NULL END) AS size_units,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    GROUP BY emd.ai2_reference)
SELECT 
    t.ai2_reference AS ai2_reference,
    CASE 
        WHEN t.size_units = 'MILLIMETRES' THEN round(t.valve_size, 0)
        WHEN t.size_units = 'CENTIMETRES' THEN round(t.valve_size * 10.0, 0)
        WHEN t.size_units = 'INCHES' THEN round(t.valve_size * 24.5, 0)
        ELSE NULL
    END AS valve_size_mm
FROM cte t
WHERE t.valve_size IS NOT NULL;

INSERT INTO temp_valve_type BY NAME
WITH cte AS 
    (SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'valve_type' THEN upper(eav.attribute_value) ELSE NULL END) AS valve_type,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    GROUP BY emd.ai2_reference)
SELECT 
    t.ai2_reference AS ai2_reference,
    t.valve_type AS valve_type,
FROM cte t
WHERE t.valve_type IS NOT NULL;

INSERT INTO temp_voltage_in BY NAME
WITH cte AS 
    (SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'voltage_in' THEN TRY_CAST(eav.attribute_value AS INTEGER) ELSE NULL END) AS voltage_in,
        any_value(CASE WHEN eav.attribute_name = 'voltage_in_ac_or_dc' THEN upper(eav.attribute_value) ELSE NULL END) AS voltage_in_ac_or_dc,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    GROUP BY emd.ai2_reference)
SELECT 
    t.ai2_reference AS ai2_reference,
    t.voltage_in AS voltage_in,
    CASE 
        WHEN t.voltage_in_ac_or_dc = 'DIRECT CURRENT' THEN 'VDC' 
        WHEN t.voltage_in_ac_or_dc = 'ALTERNATING CURRENT' THEN 'VAC'
        ELSE NULL
    END AS voltage_in_ac_or_dc
FROM cte t
WHERE t.voltage_in_ac_or_dc IS NOT NULL;


-- ## CLASS TABLES

-- EMTRIN (direct on line starter)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_emtrin BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attribute_name = 'insulation_class' THEN eav.attribute_value ELSE NULL END) AS insulation_class_deg_c,
    any_value(CASE WHEN eav.attribute_name = 'ip_rating' THEN upper(eav.attribute_value) ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attribute_name = 'current_in' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS emtr_rated_current_a,
    tmp_power.power_kilowatts AS emtr_rated_power_kw,
    any_value(CASE WHEN eav.attribute_name = 'speed_rpm' THEN TRY_CAST(eav.attribute_value AS INTEGER) ELSE NULL END) AS emtr_rated_speed_rpm,
    tmp_voltage.voltage_in AS emtr_rated_voltage,
    tmp_voltage.voltage_in_ac_or_dc AS emtr_rated_voltage_units,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
JOIN temp_voltage_in tmp_voltage ON tmp_voltage.ai2_reference = emd.ai2_reference 
JOIN temp_power tmp_power ON tmp_power.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: NON-IMMERSIBLE MOTOR'
GROUP BY emd.ai2_reference, tmp_voltage.voltage_in, tmp_voltage.voltage_in_ac_or_dc, tmp_power.power_kilowatts;


-- LSTNCO (conductive level device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lstnco BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attribute_name = 'range_max' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_range_max,
    any_value(CASE WHEN eav.attribute_name = 'range_min' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_range_min,
    any_value(CASE WHEN eav.attribute_name = 'range_unit' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_range_units,
    tmp_signal.signal_type AS lstn_signal_type,
    any_value(CASE WHEN eav.attribute_name = 'signal_unit' THEN format_output_type(eav.attribute_value) ELSE NULL END) AS lstn_output_type,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
JOIN temp_signal_type tmp_signal ON tmp_signal.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: CONDUCTIVITY LEVEL INSTRUMENT'
GROUP BY emd.ai2_reference, tmp_signal.signal_type;

-- LSTNCO (ultrasonic time of flight level device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lstnut BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    tmp_signal.signal_type AS lstn_signal_type,
    any_value(CASE WHEN eav.attribute_name = 'ip_rating' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attribute_name = 'range_max' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_range_max,
    any_value(CASE WHEN eav.attribute_name = 'range_min' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_range_min,
    any_value(CASE WHEN eav.attribute_name = 'range_unit' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_range_units,
    any_value(CASE WHEN eav.attribute_name = 'relay_1_function' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_relay_1_function,
    any_value(CASE WHEN eav.attribute_name = 'relay_1_off_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_1_off_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_1_on_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_1_on_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_2_function' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_relay_2_function,
    any_value(CASE WHEN eav.attribute_name = 'relay_2_off_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_2_off_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_2_on_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_2_on_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_3_function' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_relay_3_function,
    any_value(CASE WHEN eav.attribute_name = 'relay_3_off_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_3_off_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_3_on_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_3_on_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_4_function' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_relay_4_function,
    any_value(CASE WHEN eav.attribute_name = 'relay_4_off_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_4_off_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_4_on_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_4_on_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_5_function' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_relay_5_function,
    any_value(CASE WHEN eav.attribute_name = 'relay_5_off_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_5_off_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_5_on_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_5_on_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_6_function' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_relay_6_function,
    any_value(CASE WHEN eav.attribute_name = 'relay_6_off_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_6_off_level_m,
    any_value(CASE WHEN eav.attribute_name = 'relay_6_on_level_m' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_relay_6_on_level_m,
    any_value(CASE WHEN eav.attribute_name = 'transducer_type' THEN eav.attribute_value ELSE NULL END) AS lstn_transducer_model,
    any_value(CASE WHEN eav.attribute_name = 'transducer_serial_no' THEN eav.attribute_value ELSE NULL END) AS lstn_transducer_serial_no,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
JOIN temp_signal_type tmp_signal ON tmp_signal.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: ULTRASONIC LEVEL INSTRUMENT'
GROUP BY emd.ai2_reference, tmp_signal.signal_type;


-- STARDO (direct on line starter)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_stardo BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attribute_name = 'ip_rating' THEN eav.attribute_value ELSE NULL END) AS ip_rating,
    any_value(CASE WHEN eav.attribute_name = 'current_in' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS star_rated_current_a,
    tmp_power.power_kilowatts AS star_rated_power_kw,
    tmp_voltage.voltage_in AS star_rated_voltage,
    tmp_voltage.voltage_in_ac_or_dc AS star_rated_voltage_units,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
JOIN temp_voltage_in tmp_voltage ON tmp_voltage.ai2_reference = emd.ai2_reference 
JOIN temp_power tmp_power ON tmp_power.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: DIRECT ON LINE STARTER'
GROUP BY emd.ai2_reference, tmp_voltage.voltage_in, tmp_voltage.voltage_in_ac_or_dc, tmp_power.power_kilowatts;

-- VALVBA (ball valve)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_valvba BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    tmp_valve_size.valve_size_mm AS valv_inlet_size_mm,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
JOIN temp_valve_type tmp_valve_type ON tmp_valve_type.ai2_reference = emd.ai2_reference 
JOIN temp_valve_size tmp_valve_size ON tmp_valve_size.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: ISOLATING VALVES'
AND tmp_valve_type.valve_type = 'BALL'
GROUP BY emd.ai2_reference, tmp_valve_size.valve_size_mm;

-- VALVGA (gate valve)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_valvga BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    tmp_valve_size.valve_size_mm AS valv_inlet_size_mm,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
JOIN temp_valve_type tmp_valve_type ON tmp_valve_type.ai2_reference = emd.ai2_reference 
JOIN temp_valve_size tmp_valve_size ON tmp_valve_size.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: ISOLATING VALVES'
AND tmp_valve_type.valve_type = 'WEDGE GATE'
GROUP BY emd.ai2_reference, tmp_valve_size.valve_size_mm;

-- VALVPG (plug valve)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_valvpg BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    tmp_valve_size.valve_size_mm AS valv_inlet_size_mm,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
JOIN temp_valve_type tmp_valve_type ON tmp_valve_type.ai2_reference = emd.ai2_reference 
JOIN temp_valve_size tmp_valve_size ON tmp_valve_size.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: ISOLATING VALVES'
AND tmp_valve_type.valve_type = 'PLUG'
GROUP BY emd.ai2_reference, tmp_valve_size.valve_size_mm;