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


CREATE OR REPLACE VIEW s4_classrep.vw_flocsummary_aib_reference AS
SELECT 
    fmd.floc_id AS floc_id, 
    fmd.functional_location AS functional_location,
    fmd.floc_description AS floc_description,
    fmd.startup_date AS startup_date,
    fmd.object_type AS object_type,
    fmd.user_status AS user_status,
    fa.* EXCLUDE (floc_id),
FROM s4_classrep.floc_masterdata fmd
LEFT OUTER JOIN s4_classrep.floc_aib_reference fa ON fa.floc_id = fmd.floc_id;


CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_aib_reference AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.equi_description AS equi_description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.startup_date AS startup_date,
    emd.object_type AS object_type,
    emd.user_status AS user_status,
    ea.* EXCLUDE (equipment_id),
FROM s4_classrep.equi_masterdata emd
LEFT OUTER JOIN s4_classrep.equi_aib_reference ea ON ea.equipment_id = emd.equipment_id;


CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_asset_condition AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.equi_description AS equi_description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.startup_date AS startup_date,
    emd.object_type AS object_type,
    emd.user_status AS user_status,
    ea.* EXCLUDE (equipment_id),
FROM s4_classrep.equi_masterdata emd
LEFT OUTER JOIN s4_classrep.equi_asset_condition ea ON ea.equipment_id = emd.equipment_id;

CREATE OR REPLACE VIEW s4_classrep.vw_flocsummary_east_north AS
SELECT 
    fmd.floc_id AS floc_id, 
    fmd.functional_location AS functional_location,
    fmd.floc_description AS floc_description,
    fmd.startup_date AS startup_date,
    fmd.object_type AS object_type,
    fmd.user_status AS user_status,
    fa.* EXCLUDE (floc_id),
FROM s4_classrep.floc_masterdata fmd
LEFT OUTER JOIN s4_classrep.floc_east_north fa ON fa.floc_id = fmd.floc_id;

CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_east_north AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.equi_description AS equi_description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.startup_date AS startup_date,
    emd.object_type AS object_type,
    emd.user_status AS user_status,
    ea.* EXCLUDE (equipment_id),
FROM s4_classrep.equi_masterdata emd
LEFT OUTER JOIN s4_classrep.equi_east_north ea ON ea.equipment_id = emd.equipment_id;


CREATE OR REPLACE VIEW s4_classrep.vw_flocsummary_solution_id AS
SELECT 
    fmd.floc_id AS floc_id, 
    fmd.functional_location AS functional_location,
    fmd.floc_description AS floc_description,
    fmd.startup_date AS startup_date,
    fmd.object_type AS object_type,
    fmd.user_status AS user_status,
    fa.* EXCLUDE (floc_id),
FROM s4_classrep.floc_masterdata fmd
LEFT OUTER JOIN s4_classrep.floc_solution_id fa ON fa.floc_id = fmd.floc_id;


CREATE OR REPLACE VIEW s4_classrep.vw_equisummary_solution_id AS
SELECT 
    emd.equipment_id AS equipment_id, 
    emd.equi_description AS equi_description,
    emd.functional_location AS functional_location,
    emd.manufacturer AS manufacturer,
    emd.model_number AS model_number,
    emd.startup_date AS startup_date,
    emd.object_type AS object_type,
    emd.user_status AS user_status,
    ea.* EXCLUDE (equipment_id),
FROM s4_classrep.equi_masterdata emd
LEFT OUTER JOIN s4_classrep.equi_solution_id ea ON ea.equipment_id = emd.equipment_id;


