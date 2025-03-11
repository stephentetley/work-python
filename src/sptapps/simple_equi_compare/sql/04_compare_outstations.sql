--CREATE OR REPLACE MACRO norm_ai2_model(model) AS
--CASE 
--    WHEN model = 'POINT BLUE 4G' THEN 'POINT BLUE'
--    ELSE model
--END
--;

--CREATE OR REPLACE MACRO norm_serial_number(serial_num) AS
--    regexp_extract(serial_num, '0*(.*)$', 1)
--;

CREATE OR REPLACE VIEW telem_compare.vw_in_both AS 
WITH worklist AS
(
    -- IN BOTH
    (SELECT
        s4_site, 
        pli_num,
        manufacturer,
        norm_ai2_model(model) AS norm_model,
        norm_serial_number(serial_number),
        install_date,
    FROM telem_compare.skeleton_ai2_equi)
    INTERSECT
    (SELECT
        s4_site,
        pli_num,
        manufacturer,
        model AS norm_model,
        norm_serial_number(serial_number),
        install_date,
    FROM telem_compare.skeleton_s4_equi)
) 
SELECT 
    t.s4_site AS s4_site,
    'in-both'::inclusion_status AS record_status,
    t.pli_num AS pli_num,
    t1.equipment_id AS s4_equi_id,
    t1.equi_description AS equipment_name,
    t1.functional_location AS functional_location,
    t2.pandi_tag AS techn_id_num,
    t.manufacturer AS manfacturer,
    t1.model AS model,
    t1.specific_model_frame AS specific_model_frame,
    t1.serial_number AS serial_number,
    strftime(t1.startup_date, '%d.%m.%Y') AS install_date,
FROM worklist t
LEFT OUTER JOIN telem_raw_data.vw_s4_netw t1 ON t1.pli_num = t.pli_num AND t1.simple_status = 'OPER'
LEFT OUTER JOIN telem_raw_data.vw_ai2_outstations t2 ON t2.pli_num = t.pli_num AND t2.user_status = 'OPERATIONAL'
ORDER BY t.s4_site;


--    
CREATE OR REPLACE VIEW telem_compare.vw_missing_in_s4 AS
WITH worklist AS
(
    -- In AI2, missing from S4
    (SELECT
        s4_site, 
        pli_num,
        manufacturer,
        norm_ai2_model(model),
        norm_serial_number(serial_number),
        install_date,
    FROM telem_compare.skeleton_ai2_equi)
    EXCEPT
    (SELECT
        s4_site,
        pli_num,
        manufacturer,
        model,
        norm_serial_number(serial_number),
        install_date,
    FROM telem_compare.skeleton_s4_equi)
)
SELECT 
    t.s4_site AS s4_site,
    'missing-in-s4'::inclusion_status AS record_status,
    t.pli_num AS pli_num,
    null AS s4_equi_id,
    t1.equi_name AS equipment_name,
    null AS functional_location,
    t1.pandi_tag AS techn_id_num,
    t.manufacturer AS manfacturer,
    t1.model AS model,
    t1.specific_model_frame AS specific_model_frame,
    t1.serial_number AS serial_number,
    strftime(t1.startup_date, '%d.%m.%Y') AS install_date,    
FROM worklist t
LEFT OUTER JOIN telem_raw_data.vw_ai2_outstations t1 ON t1.pli_num = t.pli_num AND t1.user_status = 'OPERATIONAL'
ORDER BY t.s4_site;

CREATE OR REPLACE VIEW telem_compare.vw_missing_in_ai2 AS
WITH worklist AS
(
    
    -- In s4, missing from ai2 (probably ai2 is DISP)
    (SELECT
        s4_site,
        pli_num,
        manufacturer,
        model,
        norm_serial_number(serial_number),
        install_date,
    FROM telem_compare.skeleton_s4_equi)
    EXCEPT
    (SELECT
        s4_site, 
        pli_num,
        manufacturer,
        norm_ai2_model(model),
        norm_serial_number(serial_number),
        install_date,
    FROM telem_compare.skeleton_ai2_equi)
)
SELECT 
    t.s4_site AS s4_site,
    'missing-in-ai2'::inclusion_status AS record_status,
    t.pli_num AS pli_num,
    t1.equipment_id AS s4_equi_id,
    t1.equi_description AS equipment_name,
    t1.functional_location AS functional_location,
    t1.techn_id_num AS techn_id_num,
    t.manufacturer AS manfacturer,
    t1.model AS model,
    t1.specific_model_frame AS specific_model_frame,
    t1.serial_number AS serial_number,
    strftime(t1.startup_date, '%d.%m.%Y') AS install_date,    
FROM worklist t
LEFT OUTER JOIN telem_raw_data.vw_s4_netw t1 ON t1.pli_num = t.pli_num AND t1.simple_status = 'OPER'
ORDER BY t.s4_site;
;

CREATE OR REPLACE VIEW telem_compare.vw_compare_equi AS
(
    (SELECT * FROM telem_compare.vw_in_both)
    UNION
    (SELECT * FROM telem_compare.vw_missing_in_s4)
    UNION
    (SELECT * FROM telem_compare.vw_missing_in_ai2)
)
ORDER BY s4_site
;

LOAD excel;

COPY (SELECT * FROM telem_compare.vw_compare_equi) TO 'G:/work/2025/great_telemetry_reconcile/telemetry_analysis.xlsx' WITH (FORMAT xlsx, HEADER true);

