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


CREATE OR REPLACE TEMPORARY MACRO norm_siemens_model(model_name) AS (
    CASE
        WHEN model_name = 'HYDRORANGER 200  HMI' THEN 'HYDRORANGER 200'
        ELSE model_name
    END
);

CREATE OR REPLACE MACRO get_masterdata(site_reference) AS TABLE (
WITH cte1 AS (
    SELECT 
        t.equipment_id AS "AI2 PLI Number",
        t1.s4_name AS "Description",
        'I' AS "Category",
        t1.s4_floc AS "Functional Location",
        '0110' AS "Position",
        t.object_type AS "Object Type",
        t.status_of_an_object AS "Status",
    FROM s4_classrep.equi_masterdata t
    JOIN worklist_extra.worklist t1 ON t1.equipment_id = t.equipment_id
    WHERE t.equipment_id = site_reference
)
SELECT * FROM cte1);

CREATE OR REPLACE MACRO unpivot_masterdata(site_reference) AS TABLE (
WITH cte1 AS (
    SELECT 
        strftime(t.startup_date, '%d.%m.%Y') AS "Start Up Date",
        strftime(t.startup_date, '%Y') AS "Construction Year",
        strftime(t.startup_date, '%m') AS "Construction Month",
        t.manufacturer AS "Manufacturer",
        norm_siemens_model(t.model_number) AS "Model",
        t.manufact_part_number AS "Manuf. Part Number",
        t.serial_number AS "Serial Number",
        'LSTNUT' AS "Catalog Profile",
        t.technical_ident_number AS "Tech Ident Number",
    FROM s4_classrep.equi_masterdata t
    JOIN worklist_extra.worklist t1 ON t1.equipment_id = t.equipment_id
    WHERE t.equipment_id = site_reference
), cte2 AS (
    FROM cte1 UNPIVOT INCLUDE NULLS (
        attr_value FOR attr_name IN (COLUMNS(*))
    )
)
SELECT * FROM cte2);
        

CREATE OR REPLACE MACRO unpivot_lstnut(site_reference) AS TABLE (
WITH cte1 AS (
    SELECT 
        t.ip_rating AS "IP_RATING", 
        upper(t1.location_on_site) AS "LOCATION_ON_SITE",
        printf('%.0f', t.lstn_range_max) AS "LSTN_RANGE_MAX",
        printf('%.0f', t.lstn_range_min) AS "LSTN_RANGE_MIN",
        upper(t.lstn_range_units) AS "LSTN_RANGE_UNITS",
        t.lstn_relay_1_function AS "LSTN_RELAY_1_FUNCTION",
        printf('%.2f', t.lstn_relay_1_off_level_m) AS "LSTN_RELAY_1_OFF_LEVEL_M",
        printf('%.2f', t.lstn_relay_1_on_level_m) AS "LSTN_RELAY_1_ON_LEVEL_M",
        t.lstn_relay_2_function AS "LSTN_RELAY_2_FUNCTION",
        printf('%.2f', t.lstn_relay_2_off_level_m) AS "LSTN_RELAY_2_OFF_LEVEL_M",
        printf('%.2f', t.lstn_relay_2_on_level_m) AS "LSTN_RELAY_2_ON_LEVEL_M",
        t.lstn_relay_3_function AS "LSTN_RELAY_3_FUNCTION",
        printf('%.2f', t.lstn_relay_3_off_level_m) AS "LSTN_RELAY_3_OFF_LEVEL_M",
        printf('%.2f', t.lstn_relay_3_on_level_m) AS "LSTN_RELAY_3_ON_LEVEL_M",
        t.lstn_relay_4_function AS "LSTN_RELAY_4_FUNCTION",
        printf('%.2f', t.lstn_relay_4_off_level_m) AS "LSTN_RELAY_4_OFF_LEVEL_M",
        printf('%.2f', t.lstn_relay_4_on_level_m) AS "LSTN_RELAY_4_ON_LEVEL_M",
        t.lstn_relay_5_function AS "LSTN_RELAY_5_FUNCTION",
        printf('%.2f', t.lstn_relay_5_off_level_m) AS "LSTN_RELAY_5_OFF_LEVEL_M",
        printf('%.2f', t.lstn_relay_5_on_level_m) AS "LSTN_RELAY_5_ON_LEVEL_M",
        t.lstn_relay_6_function AS "LSTN_RELAY_6_FUNCTION",
        printf('%.2f', t.lstn_relay_6_off_level_m) AS "LSTN_RELAY_6_OFF_LEVEL_M",
        printf('%.2f', t.lstn_relay_6_on_level_m) AS "LSTN_RELAY_6_ON_LEVEL_M",
        t.lstn_set_to_snort AS "LSTN_SET_TO_SNORT",
        t.lstn_signal_type AS "LSTN_SIGNAL_TYPE",
        printf('%d', t.lstn_supply_voltage) AS "LSTN_SUPPLY_VOLTAGE",
        upper(t.lstn_supply_voltage_units)AS "LSTN_SUPPLY_VOLTAGE_UNITS",
        t.lstn_transducer_model AS "LSTN_TRANSDUCER_MODEL",
        t.lstn_transducer_serial_no AS "LSTN_TRANSDUCER_SERIAL_NO",
        t.lstn_transmitter_model AS "LSTN_TRANSMITTER_MODEL",
        t.lstn_transmitter_serial_no AS "LSTN_TRANSMITTER_SERIAL_NO",
        printf('%d', t.manufacturers_asset_life_yr) AS "MANUFACTURERS_ASSET_LIFE_YR",
        t.memo_line AS "MEMO_LINE",
        t.uniclass_code AS "UNICLASS_CODE",
        t.uniclass_desc AS "UNICLASS_DESC",
    FROM s4_classrep.equiclass_lstnut t
    JOIN worklist_extra.checked_location_on_site t1 ON t1.equipment_id = t.equipment_id
    WHERE t.equipment_id = site_reference
), cte2 AS (
    FROM cte1 UNPIVOT INCLUDE NULLS (
        attr_value FOR attr_name IN (COLUMNS(*))
    )
)
SELECT * FROM cte2 ORDER BY attr_name);

CREATE OR REPLACE MACRO unpivot_east_north(site_reference) AS TABLE (
WITH cte1 AS (
    SELECT 
        try_cast(t.easting AS VARCHAR) AS "EASTING", 
        try_cast(t.northing AS VARCHAR) AS "NORTHING",
    FROM s4_classrep.equi_east_north t
    WHERE equipment_id = site_reference
), cte2 AS (
    FROM cte1 UNPIVOT INCLUDE NULLS (
        attr_value FOR attr_name IN (COLUMNS(*))
    )
)
SELECT * FROM cte2 ORDER BY attr_name);


CREATE OR REPLACE MACRO unpivot_asset_condition(site_reference) AS TABLE (
WITH cte1 AS (
    SELECT 
        '1 - GOOD' AS "CONDITION_GRADE", 
        'NEW' AS "CONDITION_GRADE_REASON",
        NULL AS "LAST_REFURBISHED_DATE",
        NULL AS "SURVEY_COMMENTS",
        strftime(t.startup_date, '%Y') AS "SURVEY_DATE",
    FROM s4_classrep.equi_masterdata t
    WHERE equipment_id = site_reference
), cte2 AS (
    FROM cte1 UNPIVOT INCLUDE NULLS (
        attr_value FOR attr_name IN (COLUMNS(*))
    )
)
SELECT * FROM cte2 ORDER BY attr_name);

