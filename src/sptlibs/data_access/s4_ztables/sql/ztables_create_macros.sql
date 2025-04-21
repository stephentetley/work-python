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


CREATE OR REPLACE MACRO get_eqobj_table_data(table_name) AS TABLE (
    SELECT 
        t."Object Type" AS object_type_0,
        t."Object Type_1" AS object_type_1,        
        t."Equipment category" AS equipment_category,
        t."Remarks" AS remarks,
    FROM query_table(table_name::VARCHAR) t
);


CREATE OR REPLACE MACRO get_flobjl_table_data(table_name) AS TABLE (
    SELECT        
        t."Structure indicator" AS structure_indicator,
        t."Object Type" AS object_type_0,
        t."Object Type_1" AS object_type_1, 
        t."Remarks" AS remarks,
    FROM query_table(table_name::VARCHAR) t
);


CREATE OR REPLACE MACRO get_flocdes_table_data(table_name) AS TABLE (
    SELECT        
        t."Object Type" AS object_type,
        t."Standard FLoc Description" AS standard_floc_description,
    FROM query_table(table_name::VARCHAR) t
);

CREATE OR REPLACE MACRO get_manuf_model_table_data(table_name) AS TABLE (
    SELECT        
        t."Manufacturer" AS manufacturer,
        t."Model number" AS model_number,
    FROM query_table(table_name::VARCHAR) t
);


CREATE OR REPLACE MACRO get_obj_table_data(table_name) AS TABLE (
    SELECT 
        t."Object Type" AS object_type,
        t."Manufacturer" AS manufacturer,
        t."Remarks" AS remarks,
    FROM query_table(table_name::VARCHAR) t
);