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

-- ## stats tables

CREATE OR REPLACE MACRO pp_list(xs) AS
CASE 
    WHEN xs IS NULL THEN ''
    WHEN xs = [] THEN ''
    WHEN xs = [null] THEN ''
    ELSE try_cast(xs AS VARCHAR)
END;

-- use table macros for simple views

CREATE OR REPLACE MACRO simple_floc_summary(table_name) AS TABLE (
SELECT
    t.funcloc_id AS funcloc_id,
    t.functional_location AS functional_location,
    t.floc_description AS floc_description,
    t.startup_date AS startup_date,
    t.object_type AS object_type,
    t.display_user_status AS display_user_status,
    t1.* EXCLUDE (funcloc_id),
FROM s4_classrep.floc_masterdata t
JOIN query_table(table_name::VARCHAR) t1 ON t1.funcloc_id = t.funcloc_id
);

CREATE OR REPLACE MACRO simple_equi_summary(table_name) AS TABLE (
SELECT 
    t.equipment_id AS equipment_id, 
    t.equi_description AS equi_description,
    t.functional_location AS functional_location,
    t.manufacturer AS manufacturer,
    t.model_number AS model_number,
    t.startup_date AS startup_date,
    t.object_type AS object_type,
    t.display_user_status AS display_user_status,
    t1.* EXCLUDE (equipment_id),
FROM s4_classrep.equi_masterdata t
JOIN query_table(table_name::VARCHAR) t1 ON t1.equipment_id = t.equipment_id
);

CREATE OR REPLACE VIEW s4_classrep.vw_flocclass_stats AS
SELECT 
    array_slice(table_name, 11, len(table_name)) AS class_name,
    t.table_name AS table_name,
    t.estimated_size AS estimated_size, 
    t.estimated_size > 0 AS is_populated,
FROM duckdb_tables() t
WHERE 
    t.schema_name = 's4_classrep'
AND t.table_name LIKE 'flocclass_%';

CREATE OR REPLACE VIEW s4_classrep.vw_equiclass_stats AS
SELECT 
    array_slice(table_name, 11, len(table_name)) AS class_name,
    t.table_name AS table_name,
    t.estimated_size AS estimated_size, 
    t.estimated_size > 0 AS is_populated,
FROM duckdb_tables() t
WHERE 
    t.schema_name = 's4_classrep'
AND t.table_name LIKE 'equiclass_%';

CREATE OR REPLACE VIEW s4_classrep.vw_equishape_stats AS
SELECT 
    'sh' || regexp_extract(table_name, 'equishape_(\w{4})', 1) AS class_name,
    t.table_name AS table_name,
    t.estimated_size AS estimated_size, 
    t.estimated_size > 0 AS is_populated,
FROM duckdb_tables() t
WHERE 
    t.schema_name = 's4_classrep'
AND t.table_name LIKE 'equishape_%';


CREATE OR REPLACE VIEW s4_classrep.vw_flocsummary_aib_reference AS
WITH cte1 AS (
    SELECT 
        t.funcloc_id AS funcloc_id,
        count(t.ai2_aib_reference) AS sai_ref_count,
        min(t.ai2_aib_reference) AS sai_ref,
        list(t.ai2_aib_reference).list_filter(x -> x <> sai_ref) AS other_sai_refs,
    FROM s4_classrep.floc_aib_reference t,
    GROUP BY t.funcloc_id
)
SELECT 
    fmd.funcloc_id AS funcloc_id, 
    fmd.functional_location AS functional_location,
    fmd.floc_description AS floc_description,
    fmd.startup_date AS startup_date,
    fmd.object_type AS object_type,
    fmd.display_user_status AS display_user_status,
    ifnull(t1.sai_ref_count, 0) AS ai2_aib_reference_count,
    t1.sai_ref AS sai_ref,
    pp_list(t1.other_sai_refs) AS other_ai2_aib_refs,
FROM s4_classrep.floc_masterdata fmd
LEFT OUTER JOIN cte1 t1 ON t1.funcloc_id = fmd.funcloc_id;

CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_aib_reference AS
WITH cte1 AS (
    SELECT 
        t.equipment_id AS equipment_id,
        count(t.ai2_aib_reference) AS pli_ref_count,
        min(t.ai2_aib_reference) AS pli_ref,
        list(t.ai2_aib_reference).list_filter(x -> x <> pli_ref) AS other_pli_refs,
    FROM s4_classrep.equi_aib_reference t,
    WHERE t.ai2_aib_reference LIKE 'PLI%'
    GROUP BY t.equipment_id
), cte2 AS (
    SELECT 
        t.equipment_id AS equipment_id,
        count(t.ai2_aib_reference) AS sai_ref_count,
        min(t.ai2_aib_reference) AS sai_ref,
        list(t.ai2_aib_reference).list_filter(x -> x <> sai_ref) AS other_sai_refs,
    FROM s4_classrep.equi_aib_reference t,
    WHERE t.ai2_aib_reference NOT LIKE 'PLI%'
    GROUP BY t.equipment_id
)
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.equi_description AS equi_description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.startup_date AS startup_date,
    emd.object_type AS object_type,
    emd.display_user_status AS display_user_status,
    ifnull(t1.pli_ref_count, 0) + ifnull(t2.sai_ref_count, 0) AS ai2_aib_reference_count,
    t1.pli_ref AS pli_ref,
    t2.sai_ref AS sai_ref,
    pp_list(list_concat(t1.other_pli_refs, t2.other_sai_refs)) AS other_ai2_aib_refs,
FROM s4_classrep.equi_masterdata emd
LEFT OUTER JOIN cte1 t1 ON t1.equipment_id = emd.equipment_id
LEFT OUTER JOIN cte2 t2 ON t2.equipment_id = emd.equipment_id;


CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_asset_condition AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.equi_description AS equi_description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.startup_date AS startup_date,
    emd.object_type AS object_type,
    emd.display_user_status AS display_user_status,
    ea.* EXCLUDE (equipment_id),
FROM s4_classrep.equi_masterdata emd
LEFT OUTER JOIN s4_classrep.equi_asset_condition ea ON ea.equipment_id = emd.equipment_id;

CREATE OR REPLACE VIEW s4_classrep.vw_flocsummary_east_north AS
SELECT 
    fmd.funcloc_id AS funcloc_id, 
    fmd.functional_location AS functional_location,
    fmd.floc_description AS floc_description,
    fmd.startup_date AS startup_date,
    fmd.object_type AS object_type,
    fmd.display_user_status AS display_user_status,
    fa.* EXCLUDE (funcloc_id),
FROM s4_classrep.floc_masterdata fmd
LEFT OUTER JOIN s4_classrep.floc_east_north fa ON fa.funcloc_id = fmd.funcloc_id;

CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_east_north AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.equi_description AS equi_description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.startup_date AS startup_date,
    emd.object_type AS object_type,
    emd.display_user_status AS display_user_status,
    ea.* EXCLUDE (equipment_id),
FROM s4_classrep.equi_masterdata emd
LEFT OUTER JOIN s4_classrep.equi_east_north ea ON ea.equipment_id = emd.equipment_id;


CREATE OR REPLACE VIEW s4_classrep.vw_flocsummary_solution_id AS
WITH cte AS (
    SELECT 
        t.funcloc_id AS funcloc_id,
        count(t.solution_id) AS solution_id_count,
        list(t.solution_id) AS solution_ids,
    FROM s4_classrep.floc_solution_id t,
    GROUP BY t.funcloc_id
)
SELECT 
    fmd.funcloc_id AS funcloc_id, 
    fmd.functional_location AS functional_location,
    fmd.floc_description AS floc_description,
    fmd.startup_date AS startup_date,
    fmd.object_type AS object_type,
    fmd.display_user_status AS display_user_status,
    ifnull(fa.solution_id_count, 0) AS solution_id_count,
    pp_list(fa.solution_ids) AS solution_ids,
FROM s4_classrep.floc_masterdata fmd
LEFT OUTER JOIN cte fa ON fa.funcloc_id = fmd.funcloc_id
GROUP BY ALL;


CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_solution_id AS
WITH cte AS (
    SELECT 
        t.equipment_id AS equipment_id,
        count(t.solution_id) AS solution_id_count,
        list(t.solution_id) AS solution_ids,
    FROM s4_classrep.equi_solution_id t,
    GROUP BY t.equipment_id
)
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.equi_description AS equi_description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.startup_date AS startup_date,
    emd.object_type AS object_type,
    emd.display_user_status AS display_user_status,
    ifnull(ea.solution_id_count, 0) AS solution_id_count,
    pp_list(ea.solution_ids) AS solution_ids,
FROM s4_classrep.equi_masterdata emd
LEFT OUTER JOIN cte ea ON ea.equipment_id = emd.equipment_id
GROUP BY ALL;

