
-- Outstation
CREATE OR REPLACE VIEW wo100_batch1.vw_outstations_report AS
WITH cte_existing AS (
    SELECT 
        'ai2_existing' as data_source,
        eo.* EXCLUDE(memo_line_2, memo_line_3, memo_line_4, memo_line_5), 
    FROM wo100_batch1.equi_outstation eo
    INNER JOIN wo100_batch1.worklist_actions wa ON wa.outstation_name = eo.outstation_name 
    WHERE wa.outstation_replaced <> TRUE 
), cte_sweco AS (
    SELECT 
        'outstation_new' AS data_source,
        so.*, 
    FROM wo100_batch1.sweco_outstation so
    INNER JOIN wo100_batch1.worklist_actions wa ON wa.outstation_name = so.outstation_name
    WHERE wa.outstation_replaced = TRUE
), cte_union AS (
    SELECT * FROM cte_existing
    UNION
    SELECT * FROM cte_sweco)
SELECT 
    cu.data_source AS 'Data Source',
    cu.ai2_reference AS 'Existing Asset Reference',
    cu.outstation_name AS 'Outstation Name',
    'EQUIPMENT: TELEMETRY OUTSTATION' AS 'Asset Name',
    strftime(cu.installed_from, '%b %d %Y') AS 'Installed From Date',
    cu.asset_status AS 'Asset Status',
    cu.manufacturer AS 'Manufacturer',
    cu.model AS 'Model',
    cu.specific_model_frame AS 'Specific Model/Frame',
    cu.serial_no AS 'Serial No',
    cu.p_and_i_tag AS 'P AND I Tag No',
    cu.location_on_site AS 'Location On Site',
    cu.rtu_configuration_id AS 'RTU Configuration ID',
    cu.rtu_installation_type AS 'RTU Installation Type',
    cu.rtu_powered AS 'RTU Powered',
    strftime(cu.rtu_firmware_last_updated, '%b %d %Y') AS 'RTU Firmware last updated',
    cu.telephone AS 'Telephone',
    cu.installation_wiring_type AS 'Installation Wiring Type',
    cu.memo_line_1 AS 'Memo Line 1',
    '1 - Good' AS 'Condition Grade',
    'New' AS 'Condition Grade Reason',
    datepart('year', cu.installed_from) AS 'AGASP Survey Year',
FROM cte_union cu
ORDER BY outstation_name;

-- Modem
CREATE OR REPLACE VIEW wo100_batch1.vw_modems_report AS
SELECT 
    'ai2_existing' as 'Data Source',
    em.ai2_reference AS 'Existing Asset Reference',
    em.outstation_name AS 'Outstation Name',
    'EQUIPMENT: MODEM' AS 'Asset Name',
    strftime(em.installed_from, '%b %d %Y') AS 'Installed From Date',
    em.asset_status AS 'Asset Status',
    em.manufacturer AS 'Manufacturer',
    em.model AS 'Model',
    em.specific_model_frame AS 'Specific Model/Frame',
    em.serial_no AS 'Serial No',
    em.location_on_site AS 'Location On Site',
    '1 - Good' AS 'Condition Grade',
    'New' AS 'Condition Grade Reason',
    datepart('year', em.installed_from) AS 'AGASP Survey Year', 
FROM wo100_batch1.equi_modem em
ORDER BY outstation_name;
    
-- Controller
CREATE OR REPLACE VIEW wo100_batch1.vw_controllers_report AS
WITH cte_existing AS (
    SELECT 
        'ai2_existing' as data_source,
        ec.*, 
    FROM wo100_batch1.equi_controller ec
    INNER JOIN wo100_batch1.worklist_actions wa ON wa.outstation_name = ec.outstation_name 
    WHERE wa.controller_replaced <> TRUE 
), cte_sweco AS (
    SELECT 
        'contoller_new' AS data_source,
        sc.*, 
    FROM wo100_batch1.sweco_controller sc
    INNER JOIN wo100_batch1.worklist_actions wa ON wa.outstation_name = sc.outstation_name
    WHERE wa.controller_replaced = TRUE
), cte_union AS (
    SELECT * FROM cte_existing
    UNION
    SELECT * FROM cte_sweco)
SELECT 
    cu.data_source AS 'Data Source',
    cu.ai2_reference AS 'Existing Asset Reference',
    cu.outstation_name AS 'Outstation Name',
    'EQUIPMENT: NETWORK' AS 'Asset Name',
    strftime(cu.installed_from, '%b %d %Y') AS 'Installed From Date',
    cu.asset_status AS 'Asset Status',
    cu.manufacturer AS 'Manufacturer',
    cu.model AS 'Model',
    cu.specific_model_frame AS 'Specific Model/Frame',
    cu.serial_no AS 'Serial No',
    cu.location_on_site AS 'Location On Site',
    '1 - Good' AS 'Condition Grade',
    'New' AS 'Condition Grade Reason',
    datepart('year', cu.installed_from) AS 'AGASP Survey Year', 
FROM cte_union cu 
ORDER BY outstation_name;

-- Power Supply
CREATE OR REPLACE VIEW wo100_batch1.vw_power_supplies_report AS
SELECT 
    'ai2_existing' as 'Data Source',
    eps.ai2_reference AS 'Existing Asset Reference',
    eps.outstation_name AS 'Outstation Name',
    'EQUIPMENT: POWER SUPPLY' AS 'Asset Name',
    strftime(eps.installed_from, '%b %d %Y') AS 'Installed From Date',
    eps.asset_status AS 'Asset Status',
    eps.manufacturer AS 'Manufacturer',
    eps.model AS 'Model',
    eps.specific_model_frame AS 'Specific Model/Frame',
    eps.serial_no AS 'Serial No',
    eps.location_on_site AS 'Location On Site',
    '1 - Good' AS 'Condition Grade',
    'New' AS 'Condition Grade Reason',
    datepart('year', eps.installed_from) AS 'AGASP Survey Year', 
FROM wo100_batch1.equi_power_supply eps
ORDER BY outstation_name;
