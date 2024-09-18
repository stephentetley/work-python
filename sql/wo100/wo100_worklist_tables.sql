-- Outstation
CREATE OR REPLACE TABLE wo100_batch1.sweco_outstation AS
SELECT 
    w.asset_name AS outstation_name,
    w.o_rtu_assest_installed_from_date AS installed_from,
    'OPERATIONAL' AS asset_statue,
    'METASPHERE' AS manufacturer,
    w.o_specific_model AS model,
    w.o_specific_frame AS specific_model_frame,
    w.o_serial_no AS serial_no,
    w.asset_name AS p_and_i_tag,
    w.o_location_on_site AS location_on_site,
    w.o_rtu_configuration_id AS rtu_configuration_id,
    w.o_rtu_installation_type AS rtu_installation_type,
    w.rtu_powered AS rtu_powered,
    w.rtu_firmware_last_updated AS rtu_firmware_last_updated,
    '' AS telephone,
    w.o_installation_wiring_type AS installation_wiring_type,
    'Asset replaced by Sweco on scheme WO100' AS memo_line_1,
FROM sweco_raw_data.worklist w 
ORDER BY outstation_name
;

CREATE OR REPLACE MACRO controller_manufacturer(s) AS
    CASE 
        WHEN s = 'WAGO LTD' THEN 'WAGO'
        ELSE s
    END
;

CREATE OR REPLACE MACRO controller_model(s) AS
    CASE 
        WHEN s = 'WAGO LTD' THEN 'SYSTEM 750'
        ELSE 'Unknown'
    END
;

-- Controller
CREATE OR REPLACE TABLE wo100_batch1.sweco_controller AS
SELECT 
    w.asset_name AS outstation_name,
    w.w_controller_install_date AS installed_from,
    'OPERATIONAL' AS asset_statue,
    controller_manufacturer(w.w_controller_manufacturer) AS manufacturer,
    controller_model(w.w_controller_manufacturer) AS model,
    '' AS specific_model_frame,
    w.w_controller_serial_number AS serial_no,
    w.o_location_on_site AS location_on_site,
FROM sweco_raw_data.worklist w 
ORDER BY outstation_name
;