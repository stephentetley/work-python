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
INSERT OR REPLACE INTO s4_classrep.floc_masterdata BY NAME
SELECT 
    f.funcloc AS funcloc_id,
    CASE WHEN starts_with(f.funcloc, '$') THEN f.floc_ref ELSE f.funcloc END AS functional_location,
    TRY_CAST(f.adrnr AS INTEGER) AS address_ref,
    f.rbnr_floc AS catalog_profile,
    f.fltyp::TEXT AS category,
    f.bukrsfloc AS company_code,
    TRY_CAST(f.baumm AS INTEGER) AS construction_month,
    f.baujj AS construction_year,
    f.kokr_floc controlling_area,
    f.kost_floc AS cost_center,
    f.txtmi AS floc_description,
    TRY_CAST(f.posnr AS INTEGER) AS display_position,
    f.iequi AS installation_allowed,
    f.floc_ref AS internal_floc_ref,
    f.stor_floc AS floc_location,
    f.swerk_fl AS maintenance_plant, 
    f.gewrkfloc AS maint_work_center,
    f.eqart AS object_type,
    f.jobjn_fl AS object_number,
    f.plnt_floc AS planning_plant,
    f.beber_fl AS plant_section,
    f.inbdt AS startup_date,
    f.tplkz_flc AS structure_indicator,
    f.tplma AS superior_funct_loc,
    f.ustw_floc AS status_of_an_object,
    f.usta_floc AS display_user_status,
    f.arbplfloc AS work_center,
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY f1.floc_ref) AS rownum,
    FROM file_download.funcloc f1
    ) f
WHERE f.rownum = 1;

--- source table has _duplicates_ which cause an error without the row_number interior table 
INSERT OR REPLACE INTO s4_classrep.equi_masterdata BY NAME
SELECT 
    e.equi AS equipment_id,
    e.adrnr AS address_ref,
    e.rbnr_eeqz AS catalog_profile,
    e.eqtyp AS category,
    e.bukr_eilo AS company_code,
    e.baumm_eqi AS construction_month,
    e.baujj AS construction_year,
    e.kokr_eilo AS controlling_area,
    e.kost_eilo AS cost_center,
    e.txtmi AS equi_description,
    e.heqn_eeqz AS display_position,
    e.tpln_eilo AS functional_location,
    e.brgew AS gross_weight,
    e.stor_eilo AS equi_location,
    e.arbp_eeqz AS maint_work_center,
    e.swer_eilo AS maintenance_plant,
    e.serge AS serial_number,
    e.mapa_eeqz AS manufact_part_number,
    e.herst AS manufacturer,
    e.typbz AS model_number,
    e.eqart_equ AS object_type,
    e.ppla_eeqz AS planning_plant,
    e.bebe_eilo AS plant_section,
    e.inbdt AS startup_date,
    e.hequ_eeqz AS superord_id,
    e.ustw_equi AS status_of_an_object,
    e.tidn_eeqz AS technical_ident_number,
    e.gewei AS unit_of_weight,
    e.usta_equi AS display_user_status,
    e.data_eeqz AS valid_from,
    e.arbp_eilo AS work_center,
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY e1.equi) AS rownum,
    FROM file_download.equi e1
    ) e
WHERE e.rownum = 1;

-- ## AIB_REFERENCE (floc)

INSERT OR REPLACE INTO s4_classrep.floc_aib_reference BY NAME
SELECT DISTINCT ON (funcloc_id, value_index)
    t.funcloc AS funcloc_id,
    t.valcnt AS value_index,
    t.atwrt AS ai2_aib_reference
FROM file_download.valuafloc t
WHERE t.charid = 'AI2_AIB_REFERENCE'
AND ai2_aib_reference IS NOT NULL;

-- ## AIB_REFERENCE (equi)

INSERT OR REPLACE INTO s4_classrep.equi_aib_reference BY NAME
SELECT DISTINCT ON (equipment_id, value_index)
    t.equi AS equipment_id,
    t.valcnt AS value_index,
    t.atwrt AS ai2_aib_reference
FROM file_download.valuaequi t
WHERE t.charid = 'AI2_AIB_REFERENCE'
AND ai2_aib_reference IS NOT NULL;

-- ## ASSET_CONDITION

-- Don't add empty records so use a cte for filtering
INSERT OR REPLACE INTO s4_classrep.equi_asset_condition BY NAME
WITH cte AS (
    SELECT DISTINCT ON(e.equipment_id)   
        e.equipment_id AS equipment_id,
        any_value(CASE WHEN eav.charid = 'CONDITION_GRADE' THEN eav.atwrt ELSE NULL END) AS condition_grade,
        any_value(CASE WHEN eav.charid = 'CONDITION_GRADE_REASON' THEN eav.atwrt ELSE NULL END) AS condition_grade_reason,
        any_value(CASE WHEN eav.charid = 'SURVEY_COMMENTS' THEN eav.atwrt ELSE NULL END) AS survey_comments,
        any_value(CASE WHEN eav.charid = 'SURVEY_DATE' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS survey_date,
    FROM s4_classrep.equi_masterdata e
    JOIN file_download.valuaequi eav ON eav.equi = e.equipment_id
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

INSERT OR REPLACE INTO s4_classrep.floc_east_north BY NAME
SELECT DISTINCT ON(f.funcloc_id)   
    f.funcloc_id AS funcloc_id,
    any_value(CASE WHEN eav.charid = 'EASTING' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS easting,
    any_value(CASE WHEN eav.charid = 'NORTHING' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS northing,
FROM s4_classrep.floc_masterdata f
JOIN file_download.valuafloc eav ON eav.funcloc = f.funcloc_id
GROUP BY funcloc_id;

INSERT OR REPLACE INTO s4_classrep.equi_east_north BY NAME
SELECT DISTINCT ON(e.equipment_id)
    e.equipment_id AS equipment_id,
    any_value(CASE WHEN eav.charid = 'EASTING' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS easting,
    any_value(CASE WHEN eav.charid = 'NORTHING' THEN TRY_CAST(eav.atflv AS INTEGER) ELSE NULL END) AS northing,
FROM s4_classrep.equi_masterdata e
JOIN file_download.valuaequi eav ON eav.equi = e.equipment_id
GROUP BY equipment_id;


-- ## SOLUTION_ID

INSERT INTO s4_classrep.floc_solution_id BY NAME
SELECT DISTINCT ON (funcloc_id, value_index)
    t.funcloc AS funcloc_id,
    t.valcnt AS value_index,
    t.atwrt AS solution_id
FROM file_download.valuafloc t
WHERE t.charid = 'SOLUTION_ID'
AND solution_id IS NOT NULL;


INSERT INTO s4_classrep.equi_solution_id BY NAME
SELECT DISTINCT ON (equipment_id, value_index)
    t.equi AS equipment_id,
    t.valcnt AS value_index,
    t.atwrt AS solution_id
FROM file_download.valuaequi t
WHERE t.charid = 'SOLUTION_ID'
AND solution_id IS NOT NULL;


-- ## Equi shape classes

INSERT INTO s4_classrep.equishape_cfbm BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'DAIMETER_MM' THEN t2.atflv ELSE NULL END) AS diameter_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS side_depth_mm,
    any_value(CASE WHEN t2.charid = 'TOP_SURFACE_AREA_M2' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS top_surface_area_m2,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHCFBM'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;

INSERT INTO s4_classrep.equishape_cobm BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'CENTRE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS centre_depth_mm,
    any_value(CASE WHEN t2.charid = 'DAIMETER_MM' THEN t2.atflv ELSE NULL END) AS diameter_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS side_depth_mm,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHCOBM'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;

INSERT INTO s4_classrep.equishape_ecyl BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'LENGTH_MM' THEN t2.atflv ELSE NULL END) AS length_mm,
    any_value(CASE WHEN t2.charid = 'MAJOR_AXIS' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS major_axis_mm,
    any_value(CASE WHEN t2.charid = 'MINOR_AXIS' THEN t2.atflv ELSE NULL END) AS minor_axis_mm,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHECYL'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;

INSERT INTO s4_classrep.equishape_hcyl BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'DIAMETER_MM' THEN t2.atflv ELSE NULL END) AS diameter_mm,
    any_value(CASE WHEN t2.charid = 'LENGTH_MM' THEN t2.atflv ELSE NULL END) AS length_mm,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHHCYL'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;

INSERT INTO s4_classrep.equishape_miir BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'CENTRE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS centre_depth_mm,
    any_value(CASE WHEN t2.charid = 'DIAMETER_MM' THEN t2.atflv ELSE NULL END) AS diameter_mm,
    any_value(CASE WHEN t2.charid = 'LENGTH_MM' THEN t2.atflv ELSE NULL END) AS length_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS side_depth_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MAX_MM' THEN t2.atflv ELSE NULL END) AS side_depth_max_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MIN_MM' THEN t2.atflv ELSE NULL END) AS side_depth_min_mm,
    any_value(CASE WHEN t2.charid = 'TOP_SURFACE_AREA_M2' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS top_surface_area_m2,
    any_value(CASE WHEN t2.charid = 'WIDTH_MM' THEN t2.atflv ELSE NULL END) AS width_mm,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHMIIR'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;

INSERT INTO s4_classrep.equishape_rfbm BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'LENGTH_MM' THEN t2.atflv ELSE NULL END) AS length_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS side_depth_mm,
    any_value(CASE WHEN t2.charid = 'TOP_SURFACE_AREA_M2' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS top_surface_area_m2,
    any_value(CASE WHEN t2.charid = 'WIDTH_MM' THEN t2.atflv ELSE NULL END) AS width_mm,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHRFBM'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;

INSERT INTO s4_classrep.equishape_rpbm BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'CENTRE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS centre_depth_mm,
    any_value(CASE WHEN t2.charid = 'LENGTH_MM' THEN t2.atflv ELSE NULL END) AS length_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS side_depth_mm,
    any_value(CASE WHEN t2.charid = 'WIDTH_MM' THEN t2.atflv ELSE NULL END) AS width_mm,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHRPBM'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;

INSERT INTO s4_classrep.equishape_rsbm BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'LENGTH_MM' THEN t2.atflv ELSE NULL END) AS length_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MAX_MM' THEN t2.atflv ELSE NULL END) AS side_depth_max_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MIN_MM' THEN t2.atflv ELSE NULL END) AS side_depth_min_mm,
    any_value(CASE WHEN t2.charid = 'TOP_SURFACE_AREA_M2' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS top_surface_area_m2,
    any_value(CASE WHEN t2.charid = 'WIDTH_MM' THEN t2.atflv ELSE NULL END) AS width_mm,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHRSBM'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;

INSERT INTO s4_classrep.equishape_scyl BY NAME
SELECT DISTINCT ON(t.equipment_id)
    t.equipment_id AS equipment_id,
    any_value(CASE WHEN t2.charid = 'CAPACITY_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS capacity_m3,
    any_value(CASE WHEN t2.charid = 'DIAMETER_MM' THEN t2.atflv ELSE NULL END) AS diameter_mm,
    any_value(CASE WHEN t2.charid = 'SIDE_DEPTH_MM' THEN t2.atflv ELSE NULL END) AS side_depth_mm,
    any_value(CASE WHEN t2.charid = 'WORKING_VOLUME_M3' THEN TRY_CAST(t2.atwrt AS DECIMAL) ELSE NULL END) AS working_volume_m3,
FROM s4_classrep.equi_masterdata t
SEMI JOIN file_download.classequi t1 ON (t1.equi = t.equipment_id) AND t1.classname = 'SHSCYL'
JOIN file_download.valuaequi t2 ON t2.equi = t.equipment_id
GROUP BY equipment_id;
