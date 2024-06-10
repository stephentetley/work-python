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


INSERT OR REPLACE INTO ai2_class_rep.equi_asset_condition BY NAME
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
INSERT INTO ai2_class_rep.equi_east_north BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN eav.attribute_value ELSE NULL END) AS grid_ref,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN udf_get_easting(eav.attribute_value) ELSE NULL END) AS easting,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN udf_get_northing(eav.attribute_value) ELSE NULL END) AS northing,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT:%'
GROUP BY emd.ai2_reference;


-- # EQUICLASS TABLES

-- ## ACSTBR (Bridge)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_acstbr BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: BRIDGE'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## AERASD (Submerged Aeration Diffuser)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_aerasd BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: DIFFUSERS'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## AIRCON (Air Conditioning Unit)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_aircon BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: AIR CONDITIONING SYSTEM'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## ALAMCC (CCTV Camera)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_alamcc BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CCTV EQUIPMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## ALAMFS (Fire Alarm System)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_alamfs BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: FIRE ALARM'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## ALAMIA (Intruder Alarm)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_alamia BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: BURGLAR ALARM'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## ANALAM (Ammonia (Ion) Analyser)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_analam BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: AMMONIA INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## ANALCL (Chlorine (Ion) Analyser)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_analcl BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CHLORINE INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## ANALCO (Conductivity (in fluid) Analyser)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_analco BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CONDUCTIVITY INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## ANALDE (Density Analyser)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_analde BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: DENSITY INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## ANALDO (Dissolved Oxygen Analyser)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_analdo BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: DISSOLVED OXYGEN INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## ANALTB (Turbidity in Fluid Analyser)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_analtb BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: TURBIDITY INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## CCBKAC (Air Circuit Breaker)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_ccbkac BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: AIR CIRCUIT BREAKER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## CFUGCE (Centrifuge)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_cfugce BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CENTRIFUGE'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## CHAMBR (Chamber)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_chambr BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CHAMBER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;



-- ## COUNCP (Compactor)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_councp BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: COMPACTOR'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## COTRCT (Condensate Trap)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_cotrct BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CONDENSATE TRAP'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## CRHTBT (Beam Trolley)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_crhtbt BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: BEAM TROLLEY'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## CRHTCR (Crane)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_crhtcr BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CRANES'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## CRHTDS (Davit Socket)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_crhtds BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: DAVIT SOCKETS'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## CRHTJI (Jib Crane)
-- TODO type refinement
INSERT OR REPLACE INTO ai2_class_rep.equiclass_crhtji BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: JIB CRANE'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## DECOEB (Emergency Eye Wash Station)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_decoeb BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: EMERGENCY EYE BATH'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## DECOES (Emergency Eye Wash and Shower)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_decoes BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: EMERGENCY SHOWER + EYE BATH'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## DECOSH (Emergency Shower)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_decosh BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: EMERGENCY SHOWER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## DISTBD (Distribution Board)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_distbd BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: DISTRIBUTION BOARD'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## EMTRIN (induction motor)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_emtrin BY NAME
WITH cte AS (
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'insulation_class' THEN eav.attribute_value ELSE NULL END) AS insulation_class_deg_c,
        any_value(CASE WHEN eav.attribute_name = 'ip_rating' THEN upper(eav.attribute_value) ELSE NULL END) AS ip_rating,
        any_value(CASE WHEN eav.attribute_name = 'current_in' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS emtr_rated_current_a,
        any_value(CASE WHEN eav.attribute_name = 'speed_rpm' THEN TRY_CAST(eav.attribute_value AS INTEGER) ELSE NULL END) AS emtr_rated_speed_rpm,
        any_value(CASE WHEN eav.attribute_name = 'power' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS __power, 
        any_value(CASE WHEN eav.attribute_name = 'power_units' THEN eav.attribute_value ELSE NULL END) AS __power_units,
        power_to_killowatts(__power_units, __power) AS emtr_rated_power_kw,
        any_value(CASE WHEN eav.attribute_name = 'voltage_in' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS emtr_rated_voltage,
        any_value(CASE WHEN eav.attribute_name = 'voltage_in_ac_or_dc' THEN voltage_ac_or_dc(eav.attribute_value) ELSE NULL END) AS emtr_rated_voltage_units,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference
    WHERE emd.common_name LIKE '%EQUIPMENT: NON-IMMERSIBLE MOTOR'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## FSTNIP (Emag Insertion Probe Flow Device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_fstnip BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: INSERTION FLOW INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## FSTNOC (Open Channel (usonic) Flow Device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_fstnoc BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: ULTRASONIC FLOW INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## FSTNTM (Thermal Mass Flow Transmitter)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_fstntm BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: THERMAL MASS FLOW INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## FSTNVA (Variable Area Flow Device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_fstnva BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: VARIABLE AREA FLOW INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## HEATIM (Immersion Heater)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_heatim BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: IMMERSION HEATER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## HSYSPP (Hydraulic Power Pack)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_hsyspp BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: HYDRAULIC POWER PACK'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## INTFLO (Local Operator Interface for PLCs)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_intflo BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: L.O.I.'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## KISKKI (Kiosk)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_kiskki BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: KIOSK'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## LIACBS (Bow Shackle)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_liacbs BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: BOW SHACKLES'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## LIDEEX (Exterior Lighting)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lideex BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: BUILDING OUTSIDE LIGHTING'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## LIDEIN (Interior Lighting)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lidein BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: BUILDING INTERNAL LIGHTING'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## LISLBS (Belt Sling)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lislbs BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: BELT SLINGS'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## LISLCS (Chain Sling)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lislcs BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CHAIN SLINGS'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## LSTNCO (Conductive Level Device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lstnco BY NAME
WITH cte AS (
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'range_max' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_range_max,
        any_value(CASE WHEN eav.attribute_name = 'range_min' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_range_min,
        any_value(CASE WHEN eav.attribute_name = 'range_unit' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_range_units,
        any_value(CASE WHEN eav.attribute_name = 'signal_max' THEN eav.attribute_value ELSE NULL END) AS __signal_max,
        any_value(CASE WHEN eav.attribute_name = 'signal_min' THEN eav.attribute_value ELSE NULL END) AS __signal_min,
        any_value(CASE WHEN eav.attribute_name = 'signal_unit' THEN eav.attribute_value ELSE NULL END) AS __signal_unit,
        format_signal(__signal_min, __signal_max, __signal_unit) AS lstn_signal_type,
        format_output_type(__signal_unit) AS lstn_output_type,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CONDUCTIVITY LEVEL INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## LSTNCP (Capacitive Level Device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lstncp BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CAPACITANCE LEVEL INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## LSTNFL                             Level Float Device
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lstnfl BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: FLOAT LEVEL INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## LSTNUT (ultrasonic time of flight level device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lstnut BY NAME
WITH cte AS (
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'signal_max' THEN eav.attribute_value ELSE NULL END) AS __signal_max,
        any_value(CASE WHEN eav.attribute_name = 'signal_min' THEN eav.attribute_value ELSE NULL END) AS __signal_min,
        any_value(CASE WHEN eav.attribute_name = 'signal_unit' THEN eav.attribute_value ELSE NULL END) AS __signal_unit,
        format_signal(__signal_min, __signal_max, __signal_unit) AS lstn_signal_type,
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
    WHERE emd.common_name LIKE '%EQUIPMENT: ULTRASONIC LEVEL INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## METREL (Electricity Meter)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_metrel BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: ELECTRIC METER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## PODEUP (Uninterruptable Power Supply)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_podeup BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: UPS SYSTEMS'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## POGEAC                             Alternator
INSERT OR REPLACE INTO ai2_class_rep.equiclass_pogeac BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: GENERATOR ALTERNATOR'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## PSTNDI (Diaphragm Type Pressure Device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_pstndi BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: GAUGE PRESSURE'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## PUMPCE (Centrifugal Pump)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_pumpce BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: CENTRIFUGAL PUMP'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## PUMPDI (Diaphragm Pump)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_pumpdi BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: DIAPHRAGM PUMP'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## PUMPHR (Helical Rotor Pump)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_pumphr BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: HELICAL ROTOR PUMP'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## SFERBA (Breathing Apparatus Cylinder)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_sferba BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: B.A. CYLINDER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## SFERFA (Fall Arrester)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_sferfa BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: FALL ARRESTER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## SHWRDE (Domestic Electrical Shower)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_shwrde BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: ELECTRIC SHOWER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## STARDO (Direct On Line Starter)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_stardo BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'ip_rating' THEN eav.attribute_value ELSE NULL END) AS ip_rating,
        any_value(CASE WHEN eav.attribute_name = 'current_in' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS star_rated_current_a,
        any_value(CASE WHEN eav.attribute_name = 'power' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS __power, 
        any_value(CASE WHEN eav.attribute_name = 'power_units' THEN eav.attribute_value ELSE NULL END) AS __power_units,
        power_to_killowatts(__power_units, __power) AS star_rated_power_kw,
        any_value(CASE WHEN eav.attribute_name = 'voltage_in' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS star_rated_voltage,
        any_value(CASE WHEN eav.attribute_name = 'voltage_in_ac_or_dc' THEN voltage_ac_or_dc(eav.attribute_value) ELSE NULL END) AS star_rated_voltage_units,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: DIRECT ON LINE STARTER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## STARVF (Variable Frequency Starter)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_starvf BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'ip_rating' THEN eav.attribute_value ELSE NULL END) AS ip_rating,
        any_value(CASE WHEN eav.attribute_name = 'current_in' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS star_input_current_a,
        any_value(CASE WHEN eav.attribute_name = 'power' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS __power, 
        any_value(CASE WHEN eav.attribute_name = 'power_units' THEN eav.attribute_value ELSE NULL END) AS __power_units,
        power_to_killowatts(__power_units, __power) AS star_rated_power_kw,
        any_value(CASE WHEN eav.attribute_name = 'voltage_in' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS star_input_voltage,
        any_value(CASE WHEN eav.attribute_name = 'voltage_in_ac_or_dc' THEN voltage_ac_or_dc(eav.attribute_value) ELSE NULL END) AS star_input_voltage_units,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: VARIABLE FREQUENCY STARTER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## TSTNTT (Temperature Monitoring Device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_tstntt BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: TEMPERATURE INSTRUMENT'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## VALVAV  (Air Release Valve)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_valvav BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'size' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS __size, 
        any_value(CASE WHEN eav.attribute_name = 'size_units' THEN upper(eav.attribute_value) ELSE NULL END) AS __size_units,
        size_to_millimetres(__size_units, __size) AS valv_inlet_size_mm, 
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: AIR RELIEF VALVE'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## VALVBA (ball valve)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_valvba BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'size' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS __size, 
        any_value(CASE WHEN eav.attribute_name = 'size_units' THEN upper(eav.attribute_value) ELSE NULL END) AS __size_units,
        size_to_millimetres(__size_units, __size) AS valv_inlet_size_mm, 
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    JOIN ai2_export.equi_eav_data eav2 ON eav2.ai2_reference = eav.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: ISOLATING VALVES'
    AND eav2.attribute_name = 'valve_type' AND upper(eav2.attribute_value) = 'BALL'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## VALVBE (Bellmouth Valve)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_valvbe BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'size' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS __size, 
        any_value(CASE WHEN eav.attribute_name = 'size_units' THEN upper(eav.attribute_value) ELSE NULL END) AS __size_units,
        size_to_millimetres(__size_units, __size) AS valv_inlet_size_mm, 
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: BELLMOUTH VALVE'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;


-- ## VALVGA (gate valve)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_valvga BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'size' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS __size, 
        any_value(CASE WHEN eav.attribute_name = 'size_units' THEN upper(eav.attribute_value) ELSE NULL END) AS __size_units,
        size_to_millimetres(__size_units, __size) AS valv_inlet_size_mm, 
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    JOIN ai2_export.equi_eav_data eav2 ON eav2.ai2_reference = eav.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: ISOLATING VALVES'
    AND eav2.attribute_name = 'valve_type' AND upper(eav2.attribute_value) = 'WEDGE GATE'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## VALVPG (plug valve)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_valvpg BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'size' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS __size, 
        any_value(CASE WHEN eav.attribute_name = 'size_units' THEN upper(eav.attribute_value) ELSE NULL END) AS __size_units,
        size_to_millimetres(__size_units, __size) AS valv_inlet_size_mm, 
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    JOIN ai2_export.equi_eav_data eav2 ON eav2.ai2_reference = eav.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: ISOLATING VALVES'
    AND eav2.attribute_name = 'valve_type' AND upper(eav2.attribute_value) = 'PLUG'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## VEPRAR (Air Receiver Vessel)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_veprar BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: AIR RECEIVER'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## WELLWT (Wet Well)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_wellwt BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)   
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: WET WELL'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;
