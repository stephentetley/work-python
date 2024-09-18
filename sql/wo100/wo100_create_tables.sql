CREATE SCHEMA IF NOT EXISTS wo100_output;

CREATE OR REPLACE TABLE wo100_output.worklist_actions AS
WITH 
    cte1 AS (
        SELECT 
            w.asset_name AS outstation_name,
            string_split_regex(w.site_work_carried_out, ',\b+').list_sort() AS actions,
        FROM sweco_raw_data.worklist w
        WHERE w.site_work_carried_out IS NOT NULL
    ),
    cte2 AS (
    SELECT 
        outstation_name AS outstation_name,
        actions AS __actions,
        TRY_CAST(actions AS VARCHAR) AS actions_list,
        list_contains(__actions, 'MMIM_MK5_Replaced') AS outstation_replaced,
        list_contains(__actions, 'Site_Survey_Completed') AS surveyed,
        list_contains(__actions, 'WAGO_Controller') AS controller_replaced,
        list_contains(__actions, 'VXI_PSU_Batteries_Replaced') AS batteries_replaced,
    FROM cte1
    GROUP BY ALL
    )
SELECT 
    COLUMNS(c -> NOT starts_with(c, '__')),
FROM cte2;


CREATE OR REPLACE MACRO rtu_5_6(s) AS
    CASE 
        WHEN regexp_matches(s, '\d{6}') THEN s[1:3] || '_' || s[4:6]
        WHEN regexp_matches(s, '\d{5}') THEN s[1:2] || '_' || s[3:5]
        ELSE s
    END
;


CREATE OR REPLACE MACRO rewrite_rtu_id(s) AS 
    regexp_replace(s, ',\s?|\-|\/|\s', '_').regexp_replace('^0+', '').regexp_replace('\_0', '_').rtu_5_6()
;

CREATE OR REPLACE MACRO rtu_serial_number(s) AS
    CASE 
        WHEN regexp_matches(s, '^\d{7}$') THEN lpad(s, 9, '0')
        ELSE s
    END
;

--SELECT rewrite_rtu_id('003/056') AS rtu_id;
--SELECT rtu_5_6('123255') AS rtu_id1, rtu_5_6('12255') AS rtu_id2;

-- Outstation
CREATE OR REPLACE TABLE wo100_output.equi_outstation AS
WITH cte1 AS (
    SELECT DISTINCT ON(emd.ai2_reference)
        hash(emd.ai2_reference) AS equipment_key,
        emd.ai2_reference AS ai2_reference,
        any_value(CASE WHEN eav.attribute_name = 'p_and_i_tag_no' THEN eav.attribute_value ELSE NULL END) AS outstation_name,
        emd.installed_from AS installed_from,
        emd.asset_status AS asset_status,
        emd.manufacturer AS manufacturer,
        emd.model AS model,
        any_value(CASE WHEN eav.attribute_name = 'specific_model_frame' THEN eav.attribute_value ELSE NULL END) AS specific_model_frame,
        any_value(CASE WHEN eav.attribute_name = 'serial_no' THEN rtu_serial_number(eav.attribute_value) ELSE NULL END) AS serial_no,
        any_value(CASE WHEN eav.attribute_name = 'p_and_i_tag_no' THEN eav.attribute_value ELSE NULL END) AS p_and_i_tag,
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'rtu_configuration_id' THEN rewrite_rtu_id(eav.attribute_value) ELSE NULL END) AS rtu_configuration_id,
        any_value(CASE WHEN eav.attribute_name = 'rtu_installation_type' THEN eav.attribute_value ELSE NULL END) AS rtu_installation_type,
        any_value(CASE WHEN eav.attribute_name = 'rtu_powered' THEN eav.attribute_value ELSE NULL END) AS rtu_powered,
        any_value(CASE WHEN eav.attribute_name = 'rtu_firmware_last_updated' THEN strptime(eav.attribute_value, '%b %d %Y') ELSE NULL END) AS rtu_firmware_last_updated,
        any_value(CASE WHEN eav.attribute_name = 'telephone' THEN eav.attribute_value ELSE NULL END) AS telephone,
        any_value(CASE WHEN eav.attribute_name = 'installation_wiring_type' THEN eav.attribute_value ELSE NULL END) AS installation_wiring_type,
        any_value(CASE WHEN eav.attribute_name = 'memo_line_1' THEN eav.attribute_value ELSE NULL END) AS memo_line_1,
        any_value(CASE WHEN eav.attribute_name = 'memo_line_2' THEN eav.attribute_value ELSE NULL END) AS memo_line_2,
        any_value(CASE WHEN eav.attribute_name = 'memo_line_3' THEN eav.attribute_value ELSE NULL END) AS memo_line_3,
        any_value(CASE WHEN eav.attribute_name = 'memo_line_4' THEN eav.attribute_value ELSE NULL END) AS memo_line_4,
        any_value(CASE WHEN eav.attribute_name = 'memo_line_5' THEN eav.attribute_value ELSE NULL END) AS memo_line_5,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT:%'
    GROUP BY emd.ai2_reference, emd.installed_from, emd.asset_status, emd.manufacturer, emd.model)
SELECT * FROM cte1;



CREATE OR REPLACE MACRO modem_manufacturer(s) AS
    CASE 
        WHEN s = 'BT (VIRTUIAL ACCESS)' THEN 'VIRTUAL ACCESS'
        ELSE s
    END
;


CREATE OR REPLACE MACRO modem_model(s) AS
    CASE 
        WHEN s = 'AIRLINK LX40 - EE MOBILE' THEN 'AIRLINK ESSENTIAL'
        WHEN s = 'WAVECOM FASTRACK XTEND' THEN 'FXT009'
        WHEN s = 'GW1042M - MOBILE ONLY' THEN 'GW1000M SERIES' 
        WHEN s = 'GW7314 - ADSL - MOBILE' THEN 'GW7300 SERIES'
        ELSE s
    END
;

CREATE OR REPLACE MACRO modem_specific_model(s) AS
    CASE 
        WHEN s = 'AIRLINK LX40 - EE MOBILE' THEN 'LX40'
        ELSE s
    END
;

-- Modem
CREATE OR REPLACE TABLE wo100_output.equi_modem AS
WITH cte1 AS (
    SELECT DISTINCT ON(emd.ai2_reference)
        hash(emd.ai2_reference) AS equipment_key,
        emd.ai2_reference AS ai2_reference,
        any_value(CASE WHEN eav.attribute_name = 'p_and_i_tag_no' THEN eav.attribute_value ELSE NULL END) AS outstation_name,
        any_value(CASE WHEN eav.attribute_name = 'modem_install_date' THEN strptime(eav.attribute_value, '%b %d %Y') ELSE NULL END) AS installed_from,
        emd.asset_status AS asset_status,
        any_value(CASE WHEN eav.attribute_name = 'modem_manufacturer' THEN modem_manufacturer(eav.attribute_value) ELSE NULL END) AS manufacturer,
        any_value(CASE WHEN eav.attribute_name = 'modem_type' THEN modem_model(eav.attribute_value) ELSE NULL END) AS model,
        any_value(CASE WHEN eav.attribute_name = 'modem_type' THEN modem_specific_model(eav.attribute_value) ELSE NULL END) AS specific_model_frame,
        any_value(CASE WHEN eav.attribute_name = 'modem_serial_number' THEN eav.attribute_value ELSE NULL END) AS serial_no,
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        any_value(CASE WHEN eav.attribute_name = 'ip_address' THEN eav.attribute_value ELSE NULL END) AS ip_address,    
        any_value(CASE WHEN eav.attribute_name = 'telephone' THEN eav.attribute_value ELSE NULL END) AS telephone,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT:%'
    GROUP BY emd.ai2_reference, emd.asset_status)
SELECT * FROM cte1
WHERE manufacturer is NOT NULL;

CREATE OR REPLACE MACRO controller_manufacturer(s) AS
    CASE 
        WHEN s = 'WAGO LTD' THEN 'WAGO'
        ELSE s
    END
;

CREATE OR REPLACE MACRO controller_model(s) AS
    CASE 
        WHEN s = 'WAGO LTD' THEN 'SYSTEM 750'
        ELSE s
    END
;

-- Network / Controller
CREATE OR REPLACE TABLE wo100_output.equi_controller AS
WITH cte1 AS (
    SELECT DISTINCT ON(emd.ai2_reference)
        hash(emd.ai2_reference) AS equipment_key,
        emd.ai2_reference AS ai2_reference,
        any_value(CASE WHEN eav.attribute_name = 'p_and_i_tag_no' THEN eav.attribute_value ELSE NULL END) AS outstation_name,
        any_value(CASE WHEN eav.attribute_name = 'controller_install_date' THEN strptime(eav.attribute_value, '%b %d %Y') ELSE NULL END) AS installed_from,
        emd.asset_status AS asset_status,
        any_value(CASE WHEN eav.attribute_name = 'controller_manufacturer' THEN controller_manufacturer(eav.attribute_value) ELSE NULL END) AS manufacturer,
        any_value(CASE WHEN eav.attribute_name = 'controller_manufacturer' THEN controller_model(eav.attribute_value) ELSE NULL END) AS model,
        '' AS specific_model_frame,
        any_value(CASE WHEN eav.attribute_name = 'controller_serial_number' THEN eav.attribute_value ELSE NULL END) AS serial_no,
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
        '' AS memo_line_1,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT:%'
    GROUP BY emd.ai2_reference, emd.asset_status)
SELECT * FROM cte1 
WHERE manufacturer IS NOT NULL;

CREATE OR REPLACE MACRO psu_manufacturer(s) AS
    CASE 
        WHEN s = 'VXI LTD' THEN 'VXIPOWER'
        ELSE s
    END
;

CREATE OR REPLACE MACRO psu_model(s) AS
    CASE 
        WHEN s = '75W ORACLE III' THEN 'ORACLE'
        ELSE s
    END
;

CREATE OR REPLACE MACRO psu_specific_model(s) AS
    CASE 
        WHEN s = '75W ORACLE III' THEN '75W ORACLE III'
        ELSE s
    END
;

-- Power Supply
CREATE OR REPLACE TABLE wo100_output.equi_power_supply AS
WITH cte1 AS (
    SELECT DISTINCT ON(emd.ai2_reference)
        hash(emd.ai2_reference) AS equipment_key,
        emd.ai2_reference AS ai2_reference,
        any_value(CASE WHEN eav.attribute_name = 'p_and_i_tag_no' THEN eav.attribute_value ELSE NULL END) AS outstation_name,
        any_value(CASE WHEN eav.attribute_name = 'psu_install_date' THEN strptime(eav.attribute_value, '%b %d %Y') ELSE NULL END) AS installed_from,
        emd.asset_status AS asset_status,
        any_value(CASE WHEN eav.attribute_name = 'psu_manufacturer' THEN psu_manufacturer(eav.attribute_value) ELSE NULL END) AS manufacturer,
        any_value(CASE WHEN eav.attribute_name = 'psu_model' THEN psu_model(eav.attribute_value) ELSE NULL END) AS model,
        any_value(CASE WHEN eav.attribute_name = 'psu_model' THEN psu_specific_model(eav.attribute_value) ELSE NULL END) AS specific_model_frame,
        any_value(CASE WHEN eav.attribute_name = 'psu_serial_number' THEN eav.attribute_value ELSE NULL END) AS serial_no,
        any_value(CASE WHEN eav.attribute_name = 'location_on_site' THEN eav.attribute_value ELSE NULL END) AS location_on_site,
    FROM ai2_export.equi_master_data emd
    JOIN ai2_export.equi_eav_data eav ON eav.ai2_reference = emd.ai2_reference 
    WHERE emd.common_name LIKE '%EQUIPMENT:%'
    GROUP BY emd.ai2_reference, emd.asset_status)
SELECT * FROM cte1 
WHERE manufacturer IS NOT NULL;
