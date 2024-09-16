-- Outstation
CREATE OR REPLACE TABLE wo100_batch1.sweco_outstation AS
SELECT 
    IF(w.existing_pli IS NOT NULL, hash(w.existing_pli), NULL) AS equipment_key,
    w.existing_pli AS ai2_reference,
    w.asset_name AS outstation_name,
    w.o_installed_from_date AS installed_from,
    'OPERATIONAL' AS asset_statue,
    w.o_manufacturer AS manufacturer,
    w.o_model_name AS model,
    w.o_specific_model_frame AS specific_model_frame,
    w.o_serial_no AS serial_no,
    w.asset_name AS p_and_i_tag,
    w.o_location_on_site AS location_on_site,
    w.o_rtu_configuration_id AS rtu_configuration_id,
    w.o_rtu_installation_type AS rtu_installation_type,
    w.o_rtu_powered AS rtu_powered,
    w.o_rtu_firmaware_last_updated AS rtu_firmware_last_updated,
    '' AS telephone,
    w.o_installation_wiring_type AS installation_wiring_type,
    'Asset replaced by Sweco on scheme WO100' AS memo_line_1,
FROM sweco_raw_data.sweco_worklist w 
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
        ELSE s
    END
;

-- Controller
CREATE OR REPLACE TABLE wo100_batch1.sweco_controller AS
SELECT 
    IF(w.existing_pli IS NOT NULL, hash(w.existing_pli), NULL) AS equipment_key,
    w.existing_pli AS ai2_reference,
    w.asset_name AS outstation_name,
    w.c_installed_from_date AS installed_from,
    'OPERATIONAL' AS asset_statue,
    controller_manufacturer(w.c_manufacturer) AS manufacturer,
    w.c_model_name AS model,
    w.c_specific_model_frame AS specific_model_frame,
    w.c_serial_no AS serial_no,
    w.c_location_on_site AS location_on_site,
FROM sweco_raw_data.sweco_worklist w 
ORDER BY outstation_name
;