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

CREATE SCHEMA IF NOT EXISTS ai2_metadata;

CREATE OR REPLACE TABLE ai2_metadata.normalize_manuf (
   manuf_name VARCHAR NOT NULL,
   normed_manuf_name VARCHAR NOT NULL,
   PRIMARY KEY (manuf_name)
);

CREATE OR REPLACE TABLE ai2_metadata.normalize_model (
   model_name VARCHAR NOT NULL,
   normed_model_name VARCHAR NOT NULL,
   PRIMARY KEY (model_name)
);

INSERT INTO ai2_metadata.normalize_manuf VALUES 
   ('', '#UNKNOWN')
;

INSERT INTO ai2_metadata.normalize_model VALUES 
   ('', '#UNKNOWN')
;

CREATE OR REPLACE MACRO get_normalize_manuf_model(table_name, id_col, manuf_col, model_col) AS TABLE (
WITH cte1 AS (
        SELECT 
            COLUMNS(id_col::VARCHAR) AS equi_id,
            COLUMNS(manuf_col::VARCHAR) AS original_manuf,
            COLUMNS(model_col::VARCHAR) AS original_model,
        FROM query_table(table_name::VARCHAR)
), cte2 AS (
    SELECT 
        t.equi_id,
        t.original_manuf, 
        t.original_model,
        t1.normed_manuf_name AS norm_manuf_lookup,  
        t2.normed_model_name AS norm_model_lookup,
        CASE 
            WHEN norm_manuf_lookup IS NULL THEN original_manuf
            ELSE norm_manuf_lookup
        END AS manufacturer,  
        CASE 
            WHEN norm_model_lookup IS NULL THEN original_model
            ELSE norm_model_lookup
        END AS model,
    FROM cte1 t
    LEFT JOIN ai2_metadata.normalize_manuf t1 ON t1.manuf_name = t.original_manuf
    LEFT JOIN ai2_metadata.normalize_model t2 ON t2.model_name = t.original_model
) 
SELECT 
    equi_id,
    manufacturer,
    model,
FROM cte2
);


-- -- Example of how to call....
-- SELECT  
--     t.pli_num,
--     t.equi_name,
--     t1.*,
--     norm_serial_number(t.serial_number) AS serial_number,
-- FROM equi_compare.ai2_equipment t
-- JOIN get_normalize_manuf_model(equi_compare.ai2_equipment, 'pli_num', 'manufacturer', 'model') t1 ON t1.equi_id = t.pli_num;



