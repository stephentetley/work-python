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

CREATE SCHEMA IF NOT EXISTS s4_ztables_landing;
CREATE SCHEMA IF NOT EXISTS s4_ztables;

CREATE OR REPLACE VIEW  s4_ztables_landing.vw_table_information AS (
WITH cte1 AS (
    SELECT DISTINCT ON (t.table_name)
        'eqobj' AS ztable_name,
        t.table_name landing_table,     
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'Object Type'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'Object Type_1'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'Equipment category'
    SEMI JOIN information_schema.columns t4 ON (t4.table_name = t.table_name) AND t4.column_name = 'Remarks'
), cte2 AS (
    -- Find `flobjl`
    SELECT DISTINCT ON (t.table_name)
        'flobjl' AS ztable_name,
        t.table_name landing_table,     
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'Structure indicator'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'Object Type'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'Object Type_1'
    SEMI JOIN information_schema.columns t4 ON (t4.table_name = t.table_name) AND t4.column_name = 'Remarks'
), cte3 AS (
    -- Find `flocdes`
    SELECT DISTINCT ON (t.table_name)
        'flocdes' AS ztable_name,
        t.table_name landing_table,     
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'Object Type'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'Standard FLoc Description'
), cte4 AS (
    -- Find `manuf_model`
    SELECT DISTINCT ON (t.table_name)
        'manuf_model' AS ztable_name,
        t.table_name landing_table,     
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'Manufacturer'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'Model number'
), cte5 AS (
    -- Find `obj`
    SELECT DISTINCT ON (t.table_name)
        'obj' AS ztable_name,
        t.table_name landing_table,     
    FROM 
        information_schema.columns t
    SEMI JOIN information_schema.columns t1 ON (t1.table_name = t.table_name) AND t1.column_name = 'Object Type'
    SEMI JOIN information_schema.columns t2 ON (t2.table_name = t.table_name) AND t2.column_name = 'Manufacturer'
    SEMI JOIN information_schema.columns t3 ON (t3.table_name = t.table_name) AND t3.column_name = 'Remarks'
)
SELECT * FROM cte1 
UNION
SELECT * FROM cte2
UNION
SELECT * FROM cte3
UNION
SELECT * FROM cte4
UNION
SELECT * FROM cte5
);

CREATE OR REPLACE TABLE s4_ztables.eqobj (
    object_type_0 VARCHAR,
    object_type_1 VARCHAR,
    equipment_category VARCHAR,
    remarks VARCHAR,
);

CREATE OR REPLACE TABLE s4_ztables.flobjl (
    structure_indicator VARCHAR,
    object_type_0 VARCHAR,
    object_type_1 VARCHAR,
    equipment_category VARCHAR,
);

CREATE OR REPLACE TABLE s4_ztables.flocdes (
    object_type VARCHAR,
    standard_floc_description VARCHAR,
);


CREATE OR REPLACE TABLE s4_ztables.manuf_model (
    manufacturer VARCHAR,
    model_number VARCHAR,
);

CREATE OR REPLACE TABLE s4_ztables.obj (
    object_type VARCHAR,
    manufacturer VARCHAR,
    remarks VARCHAR,
);

