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

CREATE SCHEMA IF NOT EXISTS equi_raw_data;
CREATE SCHEMA IF NOT EXISTS equi_compare;


DROP TYPE IF EXISTS inclusion_status;
CREATE TYPE inclusion_status AS ENUM ('in-both', 'missing-in-s4', 'missing-in-ai2');



-- Includes calculated fields...
CREATE OR REPLACE TABLE equi_compare.ai2_equipment (
    pli_num VARCHAR NOT NULL,
    common_name VARCHAR,
    installation_name VARCHAR,
    s4_site_code VARCHAR,
    equi_name VARCHAR,
    equipment_type VARCHAR,
    startup_date TIMESTAMP,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    pandi_tag VARCHAR,
    display_user_status VARCHAR,
    grid_ref VARCHAR,
    PRIMARY KEY (pli_num)
);

CREATE OR REPLACE TABLE equi_compare.s4_equipment (
    equipment_id VARCHAR NOT NULL,
    equi_description VARCHAR,
    functional_location VARCHAR,
    s4_site VARCHAR,
    techn_id_num VARCHAR,
    startup_date DATE,
    equi_category VARCHAR,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    display_user_status VARCHAR,
    simple_status VARCHAR,
    object_type VARCHAR,
    address_id VARCHAR,
    pli_num VARCHAR,
    sai_num VARCHAR,
    PRIMARY KEY (equipment_id)
);


CREATE OR REPLACE VIEW equi_compare.skeleton_ai2_equi AS
SELECT 
    t.s4_site_code AS s4_site, 
    t.pli_num AS pli_num,
    t.manufacturer AS manufacturer, 
    t.model AS model,  
    t.serial_number AS serial_number, 
    strftime(t.startup_date, '%d.%m.%Y') AS install_date,
    t.equi_name AS ai2_equi_name,
FROM equi_compare.ai2_equipment t
WHERE t.display_user_status = 'OPERATIONAL'
;


CREATE OR REPLACE VIEW equi_compare.skeleton_s4_equi AS
SELECT 
    t.s4_site AS s4_site, 
    t.pli_num AS pli_num, 
    t.manufacturer AS manufacturer, 
    t.model AS model,  
    t.serial_number AS serial_number, 
    strftime(t.startup_date, '%d.%m.%Y') AS install_date,
    t.equipment_id AS s4_equi_id,
    t.equi_description AS s4_equi_name,
FROM equi_compare.s4_equipment t
WHERE t.simple_status = 'OPER'
;

CREATE OR REPLACE VIEW equi_compare.vw_in_both AS 
WITH worklist AS
(
    -- IN BOTH
    (SELECT
        t.s4_site, 
        t.pli_num,
        t1.manufacturer,
        t1.model AS norm_model,
        udfx.normalize_serial_number(t.serial_number) as norm_serial_number,
        t.install_date,
    FROM equi_compare.skeleton_ai2_equi t
    JOIN get_normalize_manuf_model(equi_compare.skeleton_ai2_equi, 'pli_num', 'manufacturer', 'model') t1 ON t1.equi_id = t.pli_num)
    INTERSECT
    (SELECT
        t.s4_site,
        t.pli_num,
        t1.manufacturer,
        t1.model AS norm_model,
        udfx.normalize_serial_number(t.serial_number) as norm_serial_number,
        t.install_date,
    FROM equi_compare.skeleton_s4_equi t
    JOIN get_normalize_manuf_model(equi_compare.skeleton_s4_equi, 'pli_num', 'manufacturer', 'model') t1 ON t1.equi_id = t.pli_num)
) 
SELECT 
    t.s4_site AS s4_site,
    'in-both'::inclusion_status AS record_status,
    t.pli_num AS pli_num,
    t1.equipment_id AS s4_equi_id,
    t1.equi_description AS equipment_name,
    t1.functional_location AS functional_location,
    t2.pandi_tag AS techn_id_num,
    t1.manufacturer AS manfacturer,
    t1.model AS model,
    t1.specific_model_frame AS specific_model_frame,
    t1.serial_number AS serial_number,
    strftime(t1.startup_date, '%d.%m.%Y') AS install_date,
FROM worklist t
LEFT OUTER JOIN equi_compare.s4_equipment t1 ON t1.pli_num = t.pli_num AND t1.simple_status = 'OPER'
LEFT OUTER JOIN equi_compare.ai2_equipment t2 ON t2.pli_num = t.pli_num AND t2.display_user_status = 'OPERATIONAL'
ORDER BY t.s4_site;


--    
CREATE OR REPLACE VIEW equi_compare.vw_missing_in_s4 AS
WITH worklist AS
(
    -- In AI2, missing from S4
    (SELECT
        t.s4_site, 
        t.pli_num,
        t1.manufacturer,
        t1.model,
        udfx.normalize_serial_number(t.serial_number) as norm_serial_number,
        t.install_date,
    FROM equi_compare.skeleton_ai2_equi t
    JOIN get_normalize_manuf_model(equi_compare.skeleton_ai2_equi, 'pli_num', 'manufacturer', 'model') t1 ON t1.equi_id = t.pli_num)
    EXCEPT
    (SELECT
        t.s4_site,
        t.pli_num,
        t1.manufacturer,
        t1.model,
        udfx.normalize_serial_number(t.serial_number) as norm_serial_number,
        t.install_date,
    FROM equi_compare.skeleton_s4_equi t
    JOIN get_normalize_manuf_model(equi_compare.skeleton_s4_equi, 'pli_num', 'manufacturer', 'model') t1 ON t1.equi_id = t.pli_num)
)
SELECT 
    t.s4_site AS s4_site,
    'missing-in-s4'::inclusion_status AS record_status,
    t.pli_num AS pli_num,
    null AS s4_equi_id,
    t1.equi_name AS equipment_name,
    null AS functional_location,
    t1.pandi_tag AS techn_id_num,
    t1.manufacturer AS manfacturer,
    t1.model AS model,
    t1.specific_model_frame AS specific_model_frame,
    t1.serial_number AS serial_number,
    strftime(t1.startup_date, '%d.%m.%Y') AS install_date,    
FROM worklist t
LEFT OUTER JOIN equi_compare.ai2_equipment t1 ON t1.pli_num = t.pli_num AND t1.display_user_status = 'OPERATIONAL'
ORDER BY t.s4_site;

CREATE OR REPLACE VIEW equi_compare.vw_missing_in_ai2 AS
WITH worklist AS
(
    
    -- In s4, missing from ai2 (probably ai2 is DISP)
    (SELECT
        t.s4_site,
        t.pli_num,
        t1.manufacturer,
        t1.model,
        udfx.normalize_serial_number(t.serial_number) as norm_serial_number,
        t.install_date,
    FROM equi_compare.skeleton_s4_equi t
    JOIN get_normalize_manuf_model(equi_compare.skeleton_s4_equi, 'pli_num', 'manufacturer', 'model') t1 ON t1.equi_id = t.pli_num)
    EXCEPT
    (SELECT
        t.s4_site, 
        t.pli_num,
        t1.manufacturer,
        t1.model,
        udfx.normalize_serial_number(t.serial_number) as norm_serial_number,
        t.install_date,
    FROM equi_compare.skeleton_ai2_equi t
    JOIN get_normalize_manuf_model(equi_compare.skeleton_ai2_equi, 'pli_num', 'manufacturer', 'model') t1 ON t1.equi_id = t.pli_num)
)
SELECT 
    t.s4_site AS s4_site,
    'missing-in-ai2'::inclusion_status AS record_status,
    t.pli_num AS pli_num,
    t1.equipment_id AS s4_equi_id,
    t1.equi_description AS equipment_name,
    t1.functional_location AS functional_location,
    t1.techn_id_num AS techn_id_num,
    t1.manufacturer AS manfacturer,
    t1.model AS model,
    t1.specific_model_frame AS specific_model_frame,
    t1.serial_number AS serial_number,
    strftime(t1.startup_date, '%d.%m.%Y') AS install_date,    
FROM worklist t
LEFT OUTER JOIN equi_compare.s4_equipment t1 ON t1.pli_num = t.pli_num AND t1.simple_status = 'OPER'
ORDER BY t.s4_site;
;

CREATE OR REPLACE VIEW equi_compare.vw_compare_equi AS
(
    (SELECT * FROM equi_compare.vw_in_both)
    UNION
    (SELECT * FROM equi_compare.vw_missing_in_s4)
    UNION
    (SELECT * FROM equi_compare.vw_missing_in_ai2)
)
ORDER BY s4_site
;
