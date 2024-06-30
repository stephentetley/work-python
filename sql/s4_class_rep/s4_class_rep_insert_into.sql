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



--- source table has _duplicates_ which cause an error without the row_number interior table 
INSERT OR REPLACE INTO s4_class_rep.floc_master_data BY NAME
SELECT 
    f.funcloc AS floc_id,
    CASE WHEN starts_with(f.funcloc, '$') THEN f.floc_ref ELSE f.funcloc END AS functional_location,
    TRY_CAST(f.adrnr AS INTEGER) AS address_ref,
    f.rbnr_floc AS catalog_profile,
    f.fltyp::TEXT AS category,
    TRY_CAST(f.bukrsfloc AS INTEGER) AS company_code,
    TRY_CAST(f.baumm AS INTEGER) AS construction_month,
    TRY_CAST(f.baujj AS INTEGER) AS construction_year,
    TRY_CAST(f.kokr_floc AS INTEGER) controlling_area,
    TRY_CAST(f.kost_floc AS INTEGER) AS cost_center,
    f.txtmi AS floc_description,
    TRY_CAST(f.posnr AS INTEGER) AS display_position,
    IF(f.iequi = 'X', true, false) AS installation_allowed,
    f.floc_ref AS internal_floc_ref,
    f.stor_floc AS floc_location,
    f.swerk_fl AS maintenance_plant, 
    f.gewrkfloc AS main_work_center,
    f.eqart AS object_type,
    f.jobjn_fl AS object_number,
    TRY_CAST(f.plnt_floc AS INTEGER) AS planning_plant,
    f.beber_fl AS plant_section,
    strptime(f.inbdt, '%d.%m.%Y') AS startup_date,
    f.tplkz_flc AS structure_indicator,
    f.tplma AS superior_funct_loc,
    f.ustw_floc AS system_status,
    f.usta_floc AS user_status,
    f.arbplfloc AS work_center,
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY f1.floc_ref) AS rownum,
    FROM s4_fd_raw_data.funcloc_floc1 f1
    ) f
WHERE f.rownum = 1;

--- source table has _duplicates_ which cause an error without the row_number interior table 
INSERT OR REPLACE INTO s4_class_rep.equi_master_data BY NAME
SELECT 
    e.equi AS equipment_id,
    TRY_CAST(e.adrnr AS INTEGER) AS address_ref,
    e.rbnr_eeqz AS catalog_profile,
    e.eqtyp AS category,
    TRY_CAST(e.bukr_eilo AS INTEGER) AS company_code,
    TRY_CAST(e.baumm_eqi AS INTEGER) AS construction_month,
    TRY_CAST(e.baujj AS INTEGER) AS construction_year,
    TRY_CAST(e.kokr_eilo AS INTEGER) AS controlling_area,
    TRY_CAST(e.kost_eilo AS INTEGER) AS cost_center,
    e.txtmi AS equi_description,
    TRY_CAST(e.heqn_eeqz AS INTEGER) AS display_position,
    e.tpln_eilo AS functional_location,
    TRY_CAST(e.brgew AS DECIMAL) AS gross_weight,
    e.stor_eilo AS equi_location,
    e.arbp_eeqz AS main_work_center,
    TRY_CAST(e.swer_eilo AS INTEGER) AS maintenance_plant,
    e.serge AS serial_number,
    e.mapa_eeqz AS manufact_part_number,
    e.herst AS manufacturer,
    e.typbz AS model_number,
    e.eqart_equ AS object_type,
    TRY_CAST(e.ppla_eeqz AS INTEGER) AS planning_plant,
    e.bebe_eilo AS plant_section,
    strptime(e.inbdt, '%d.%m.%Y') AS startup_date,
    e.hequ_eeqz AS superord_id,
    e.ustw_equi AS system_status,
    e.tidn_eeqz AS technical_ident_number,
    e.gewei AS unit_of_weight,
    e.usta_equi AS user_status,
    strptime(e.data_eeqz, '%d.%m.%Y') AS valid_from,
    e.arbp_eilo AS work_center,
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY e1.equi) AS rownum,
    FROM s4_fd_raw_data.equi_equi1 e1
    ) e
WHERE e.rownum = 1;

-- ## AIB_REFERENCE (floc)

INSERT OR REPLACE INTO s4_class_rep_staging.floc_ai2_sai_references BY NAME
SELECT DISTINCT ON(f.floc_id)   
    f.floc_id AS floc_id,
    list_sort(array_agg(eav.atwrt)) AS ai2_aib_references,
FROM s4_class_rep.floc_master_data f
JOIN s4_fd_raw_data.valuafloc_valuafloc1 eav ON eav.funcloc = f.floc_id
WHERE eav.charid = 'AI2_AIB_REFERENCE'
GROUP BY floc_id;


INSERT OR REPLACE INTO s4_class_rep_staging.floc_s4_aib_reference BY NAME
SELECT DISTINCT ON(f.floc_id)   
    f.floc_id AS floc_id,
    any_value(eav.atwrt) AS s4_aib_reference,
FROM s4_class_rep.floc_master_data f
JOIN s4_fd_raw_data.valuafloc_valuafloc1 eav ON eav.funcloc = f.floc_id
WHERE eav.charid = 'S4_AIB_REFERENCE'
GROUP BY floc_id;

INSERT OR REPLACE INTO s4_class_rep.floc_aib_reference BY NAME
SELECT DISTINCT ON(f.floc_id)
    f.floc_id AS floc_id,
    sai.ai2_aib_references AS ai2_aib_references,
    s4.s4_aib_reference AS s4_aib_reference,
FROM s4_class_rep.floc_master_data f
LEFT JOIN s4_class_rep_staging.floc_ai2_sai_references sai ON sai.floc_id = f.floc_id
LEFT JOIN s4_class_rep_staging.floc_s4_aib_reference s4 ON s4.floc_id = f.floc_id;

-- ## AIB_REFERENCE (equi)

INSERT OR REPLACE INTO s4_class_rep_staging.equi_ai2_sai_reference  BY NAME
WITH cte AS (
    SELECT  
        e.equipment_id AS equipment_id,
        eav.valcnt AS value_index,
        eav.atwrt AS ai2_sai_reference,
    FROM s4_class_rep.equi_master_data e
    JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
    WHERE eav.charid = 'AI2_AIB_REFERENCE'
    AND NOT starts_with(ai2_sai_reference, 'PLI') 
    ORDER BY equipment_id ASC, value_index ASC
)
SELECT 
    equipment_id, 
    any_value(ai2_sai_reference) AS ai2_sai_reference,
    min(value_index) AS value_index
FROM cte
GROUP BY equipment_id;

INSERT OR REPLACE INTO s4_class_rep_staging.equi_ai2_pli_reference BY NAME
WITH cte AS (
    SELECT  
        e.equipment_id AS equipment_id,
        eav.valcnt AS value_index,
        eav.atwrt AS ai2_pli_reference,
    FROM s4_class_rep.equi_master_data e
    JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
    WHERE eav.charid = 'AI2_AIB_REFERENCE'
    AND starts_with(ai2_pli_reference, 'PLI') 
    ORDER BY equipment_id ASC, value_index ASC
)
SELECT 
    equipment_id, 
    any_value(ai2_pli_reference) AS ai2_pli_reference,
    min(value_index) AS value_index
FROM cte
GROUP BY equipment_id;

INSERT OR REPLACE INTO s4_class_rep.equi_aib_reference BY NAME
SELECT DISTINCT ON(e.equipment_id)
    e.equipment_id AS equipment_id,
    sai.value_index AS sai_value_index,
    sai.ai2_sai_reference AS ai2_sai_reference,
    pli.value_index AS pli_value_index,
    pli.ai2_pli_reference AS ai2_pli_reference,
    s4.s4_aib_reference AS s4_aib_reference,
    extras.extra_aib_references AS ai2_extra_references,
FROM s4_class_rep.equi_master_data e
LEFT JOIN s4_class_rep_staging.equi_ai2_sai_reference sai ON sai.equipment_id = e.equipment_id
LEFT JOIN s4_class_rep_staging.equi_ai2_pli_reference pli ON pli.equipment_id = e.equipment_id
LEFT JOIN s4_class_rep_staging.vw_equi_ai2_extra_references extras ON extras.equipment_id = e.equipment_id
LEFT JOIN s4_class_rep_staging.vw_equi_s4_aib_references s4 ON s4.equipment_id = e.equipment_id;

-- ## ASSET_CONDITION

-- Don't add empty records so use a cte for filtering
INSERT OR REPLACE INTO s4_class_rep.equi_asset_condition BY NAME
WITH cte AS (
    SELECT DISTINCT ON(e.equipment_id)   
        e.equipment_id AS equipment_id,
        any_value(CASE WHEN eav.charid = 'CONDITION_GRADE' THEN eav.atwrt ELSE NULL END) AS condition_grade,
        any_value(CASE WHEN eav.charid = 'CONDITION_GRADE_REASON' THEN eav.atwrt ELSE NULL END) AS condition_grade_reason,
        any_value(CASE WHEN eav.charid = 'SURVEY_COMMENTS' THEN eav.atwrt ELSE NULL END) AS survey_comments,
        any_value(CASE WHEN eav.charid = 'SURVEY_DATE' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS survey_date,
    FROM s4_class_rep.equi_master_data e
    JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
    GROUP BY equipment_id
)
SELECT 
    equipment_id,
    condition_grade,
    condition_grade_reason,
    survey_comments,
    survey_date,
FROM cte
WHERE condition_grade IS NOT NULL OR condition_grade_reason IS NOT NULL OR survey_comments IS NOT NULL OR survey_date IS NOT NULL; 

-- ## EAST_NORTH 

INSERT OR REPLACE INTO s4_class_rep.floc_east_north BY NAME
SELECT DISTINCT ON(f.floc_id)   
    f.floc_id AS floc_id,
    any_value(CASE WHEN eav.charid = 'EASTING' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS easting,
    any_value(CASE WHEN eav.charid = 'NORTHING' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS northing,
FROM s4_class_rep.floc_master_data f
JOIN s4_fd_raw_data.valuafloc_valuafloc1 eav ON eav.funcloc = f.floc_id
GROUP BY floc_id;

INSERT OR REPLACE INTO s4_class_rep.equi_east_north BY NAME
SELECT DISTINCT ON(e.equipment_id)
    e.equipment_id AS equipment_id,
    any_value(CASE WHEN eav.charid = 'EASTING' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS easting,
    any_value(CASE WHEN eav.charid = 'NORTHING' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS northing,
FROM s4_class_rep.equi_master_data e
JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
GROUP BY equipment_id;


-- ## SOLUTION_ID

INSERT OR REPLACE INTO s4_class_rep.floc_solution_id BY NAME
SELECT DISTINCT ON(f.floc_id)   
    f.floc_id AS floc_id,
    list_sort(array_agg(eav.atwrt)) AS solution_ids,
FROM s4_class_rep.floc_master_data f
JOIN s4_fd_raw_data.valuafloc_valuafloc1 eav ON eav.funcloc = f.floc_id
WHERE eav.charid = 'SOLUTION_ID'
GROUP BY floc_id;


INSERT OR REPLACE INTO s4_class_rep.equi_solution_id BY NAME
SELECT DISTINCT ON (e.equipment_id)
    e.equipment_id AS equipment_id,
    list_sort(array_agg(eav.atwrt)) AS solution_ids,
FROM s4_class_rep.equi_master_data e
JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
WHERE eav.charid = 'SOLUTION_ID'
GROUP BY equipment_id;

