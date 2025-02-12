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

-- ## NETWMO (Network Device connecting to Telecoms) / from EQUIPMENT: TELEMETRY OUTSTATION

--- source table may have _duplicates_ hence use `DISTINCT ON`...
INSERT INTO ai2_class_rep.equi_master_data BY NAME
SELECT DISTINCT ON (emd.ai2_reference)
    hash(emd.ai2_reference || '_outstation_modem') AS equipment_key,
    emd.ai2_reference AS ai2_reference,
    emd.common_name AS common_name,
    regexp_extract(emd.common_name, '/([^/]*)/EQUIPMENT:', 1) || ' (modem)' AS equipment_name,
    '!!OUTSTATION MODEM' AS equipment_type,
    any_value(CASE WHEN eav.attribute_name = 'modem_install_date' THEN TRY_CAST(try_strptime(eav.attribute_value, '%b %-d %Y') AS DATE) ELSE NULL END) AS installed_from,
    emd.asset_status AS asset_status,
    any_value(CASE WHEN eav.attribute_name = 'modem_manufacturer' THEN eav.attribute_value ELSE NULL END) AS manufacturer,
    any_value(CASE WHEN eav.attribute_name = 'modem_type' THEN eav.attribute_value ELSE NULL END) AS model,
    any_value(CASE WHEN eav.attribute_name = 'modem_serial_number' THEN eav.attribute_value ELSE NULL END) AS serial_number,
    any_value(CASE WHEN eav.attribute_name = 'work_centre' THEN eav.attribute_value ELSE NULL END) AS work_centre,
    any_value(CASE WHEN eav.attribute_name = 'responsible_officer' THEN eav.attribute_value ELSE NULL END) AS responsible_officer,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference
WHERE emd.common_name LIKE '%EQUIPMENT: TELEMETRY OUTSTATION'
GROUP BY emd.ai2_reference, emd.common_name, emd.asset_status;


-- uses scalar udfs `udf_get_easting` and `udf_get_northing` that must be registered first...
INSERT INTO ai2_class_rep.equi_east_north BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    hash(emd.ai2_reference || '_outstation_modem') AS equipment_key,
    emd.ai2_reference AS ai2_reference,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN eav.attribute_value ELSE NULL END) AS grid_ref,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN udf_get_easting(eav.attribute_value) ELSE NULL END) AS easting,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN udf_get_northing(eav.attribute_value) ELSE NULL END) AS northing,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: TELEMETRY OUTSTATION'
GROUP BY emd.ai2_reference;


INSERT OR REPLACE INTO ai2_class_rep.equiclass_netwmo BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        hash(emd.ai2_reference || '_outstation_modem') AS equipment_key,
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: TELEMETRY OUTSTATION'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;

-- ## PODETU (Network Device connecting to Telecoms) / from EQUIPMENT: TELEMETRY OUTSTATION

--- source table may have _duplicates_ hence use `DISTINCT ON`...
INSERT INTO ai2_class_rep.equi_master_data BY NAME
SELECT DISTINCT ON (emd.ai2_reference)
    hash(emd.ai2_reference || '_outstation_psu') AS equipment_key,
    emd.ai2_reference AS ai2_reference,
    emd.common_name AS common_name,
    regexp_extract(emd.common_name, '/([^/]*)/EQUIPMENT:', 1) || ' (modem)' AS equipment_name,
    '!!OUTSTATION PSU' AS equipment_type,
    any_value(CASE WHEN eav.attribute_name = 'psu_install_date' THEN TRY_CAST(try_strptime(eav.attribute_value, '%b %-d %Y') AS DATE) ELSE NULL END) AS installed_from,
    emd.asset_status AS asset_status,
    any_value(CASE WHEN eav.attribute_name = 'psu_manufacturer' THEN eav.attribute_value ELSE NULL END) AS manufacturer,
    any_value(CASE WHEN eav.attribute_name = 'psu_type' THEN eav.attribute_value ELSE NULL END) AS model,
    any_value(CASE WHEN eav.attribute_name = 'psu_serial_number' THEN eav.attribute_value ELSE NULL END) AS serial_number,
    any_value(CASE WHEN eav.attribute_name = 'work_centre' THEN eav.attribute_value ELSE NULL END) AS work_centre,
    any_value(CASE WHEN eav.attribute_name = 'responsible_officer' THEN eav.attribute_value ELSE NULL END) AS responsible_officer,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference
WHERE emd.common_name LIKE '%EQUIPMENT: TELEMETRY OUTSTATION'
GROUP BY emd.ai2_reference, emd.common_name, emd.asset_status;


-- uses scalar udfs `udf_get_easting` and `udf_get_northing` that must be registered first...
INSERT INTO ai2_class_rep.equi_east_north BY NAME
SELECT DISTINCT ON(emd.ai2_reference)
    hash(emd.ai2_reference || '_outstation_psu') AS equipment_key,
    emd.ai2_reference AS ai2_reference,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN eav.attribute_value ELSE NULL END) AS grid_ref,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN udf_get_easting(eav.attribute_value) ELSE NULL END) AS easting,
    any_value(CASE WHEN eav.attribute_name = 'loc_ref' THEN udf_get_northing(eav.attribute_value) ELSE NULL END) AS northing,
FROM ai2_export.equi_master_data emd
JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
WHERE emd.common_name LIKE '%EQUIPMENT: TELEMETRY OUTSTATION'
GROUP BY emd.ai2_reference;


INSERT OR REPLACE INTO ai2_class_rep.equiclass_podetu BY NAME
WITH cte AS(
    SELECT DISTINCT ON(emd.ai2_reference)
        hash(emd.ai2_reference || '_outstation_psu') AS equipment_key,
        emd.ai2_reference AS ai2_reference, 
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT: TELEMETRY OUTSTATION'
    GROUP BY emd.ai2_reference)
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__'))
FROM cte;
