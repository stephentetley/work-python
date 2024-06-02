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
    any_value(CASE WHEN eav.attribute_name = 'work_centre' THEN TRY_CAST(eav.attribute_value AS INTEGER) ELSE NULL END) AS work_centre,
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

-- Fill temp tables

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



-- INSTRUMENT

-- INSTRUMENT: LSTNCO (conductive level device)
INSERT OR REPLACE INTO ai2_class_rep.equiclass_lstnco BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    emd.ai2_reference AS ai2_reference, 
    any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    any_value(CASE WHEN eav.attribute_name = 'range_max' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_range_max,
    any_value(CASE WHEN eav.attribute_name = 'range_min' THEN TRY_CAST(eav.attribute_value AS DECIMAL) ELSE NULL END) AS lstn_range_min,
    any_value(CASE WHEN eav.attribute_name = 'range_unit' THEN upper(eav.attribute_value) ELSE NULL END) AS lstn_range_units,
    temp.signal_type AS lstn_signal_type,
    any_value(CASE WHEN eav.attribute_name = 'signal_unit' THEN format_output_type(eav.attribute_value) ELSE NULL END) AS lstn_output_type,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
JOIN temp_signal_type temp ON temp.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: CONDUCTIVITY LEVEL INSTRUMENT'
GROUP BY emd.ai2_reference, temp.signal_type;

