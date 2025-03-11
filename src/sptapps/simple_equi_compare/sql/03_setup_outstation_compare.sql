CREATE SCHEMA IF NOT EXISTS telem_compare;

CREATE OR REPLACE VIEW telem_compare.skeleton_ai2_equi AS
SELECT 
    t1.s4_site_code AS s4_site, 
    t.pli_num AS pli_num,
    t.manufacturer AS manufacturer, 
    t.model AS model,  
    t.serial_number AS serial_number, 
    strftime(t.startup_date, '%d.%m.%Y') AS install_date,
    t.equi_name AS ai2_equi_name,
    
FROM telem_raw_data.vw_ai2_outstations t
LEFT JOIN telem_raw_data.vw_site_mapping t1 ON t1.installation_name = t.installation_name
WHERE t.user_status = 'OPERATIONAL'
;


CREATE OR REPLACE VIEW telem_compare.skeleton_s4_equi AS
SELECT 
    t.s4_site AS s4_site, 
    t.pli_num AS pli_num, 
    t.manufacturer AS manufacturer, 
    t.model AS model,  
    t.serial_number AS serial_number, 
    strftime(t.startup_date, '%d.%m.%Y') AS install_date,
    t.equipment_id AS s4_equi_id,
    t.equi_description AS s4_equi_name,
FROM telem_raw_data.vw_s4_netw t
WHERE t.simple_status = 'OPER'
;



