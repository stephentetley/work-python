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


CREATE OR REPLACE MACRO get_glob_matches(glob_pattern) AS TABLE (
   SELECT 
       row_number() OVER () AS file_index, 
       t.file AS file_path,
       parse_filename(file_path, 'false', 'system') AS file_name,
   FROM glob(glob_pattern::VARCHAR) t
);

CREATE OR REPLACE MACRO get_raw_download_table(file_name) AS TABLE (
SELECT 
    COLUMNS('(\w.*)') AS '\1',
FROM read_csv(file_name, delim='\t', all_varchar=true)
);

CREATE OR REPLACE TEMPORARY MACRO get_funcloc_landing_table(table_name) AS TABLE (
    SELECT 
        t.* EXCLUDE("ANSDT", "CGWLDT_FL", "VGWLDT_FL", "IEQUI", "INBDT", "DATBI_FLO", "CGWLEN_FL", "VGWLEN_FL"),
        try_strptime(t."ANSDT", '%d.%m.%Y') AS ansdt,
        try_strptime(t."CGWLDT_FL", '%d.%m.%Y') AS cgwldt_fl,
        try_strptime(t."VGWLDT_FL", '%d.%m.%Y') AS vgwldt_fl,
        CASE 
            WHEN t."IEQUI" = 'X' THEN true
            ELSE false
        END AS iequi, 
        try_strptime(t."INBDT", '%d.%m.%Y') AS inbdt, 
        try_strptime(t."DATBI_FLO", '%d.%m.%Y') AS datbi_flo, 
        try_strptime(t."CGWLEN_FL", '%d.%m.%Y') AS cgwlen_fl,
        try_strptime(t."VGWLEN_FL", '%d.%m.%Y') AS vgwlen_fl,
    FROM query_table(table_name::VARCHAR) t
);

CREATE OR REPLACE TEMPORARY MACRO get_classfloc_landing_table(table_name) AS TABLE (
    SELECT 
        t.* EXCLUDE("CLASS"),
        t."CLASS" AS classname,    
    FROM query_table(table_name::VARCHAR) t
);

CREATE OR REPLACE TEMPORARY MACRO get_valuafloc_landing_table(table_name) AS TABLE (
    SELECT 
        t.* 
    FROM query_table(table_name::VARCHAR) t
);

CREATE OR REPLACE TEMPORARY MACRO get_equi_landing_table(table_name) AS TABLE (
    SELECT 
        t.* EXCLUDE("ANSDT", "CGWLDT_EQ", "VGWLDT_EQ", "AULDT_EQI", "INBDT", "DATA_EEQZ", "DATB_EEQZ", "DATBI_EIL", "CGWLEN_EQ", "VGWLEN_EQ", "INSDATE"),
        try_strptime(t."ANSDT", '%d.%m.%Y') AS ansdt,
        try_strptime(t."CGWLDT_EQ", '%d.%m.%Y') AS cgwldt_eq,
        try_strptime(t."VGWLDT_EQ", '%d.%m.%Y') AS vgwldt_eq, 
        try_strptime(t."AULDT_EQI", '%d.%m.%Y') AS auldt_eqi, 
        try_strptime(t."INBDT", '%d.%m.%Y') AS inbdt, 
        try_strptime(t."DATA_EEQZ", '%d.%m.%Y') AS data_eeqz, 
        try_strptime(t."DATB_EEQZ", '%d.%m.%Y') AS datb_eeqz, 
        try_strptime(t."DATBI_EIL", '%d.%m.%Y') AS datbi_eil,
        try_strptime(t."CGWLEN_EQ", '%d.%m.%Y') AS cgwlen_eq,
        try_strptime(t."VGWLEN_EQ", '%d.%m.%Y') AS vgwlen_eq,
        try_strptime(t."INSDATE", '%d.%m.%Y') AS insdate, 
    FROM query_table(table_name::VARCHAR) t
);

CREATE OR REPLACE TEMPORARY MACRO get_classequi_landing_table(table_name) AS TABLE (
    SELECT 
        t.* EXCLUDE("CLASS", "ZZSTDCLAS"),
        t."CLASS" AS classname,
        CASE 
            WHEN t."ZZSTDCLAS" = 'X' THEN true
            ELSE false
        END AS zzstdclas,        
    FROM query_table(table_name::VARCHAR) t
);


CREATE OR REPLACE TEMPORARY MACRO get_valuaequi_landing_table(table_name) AS TABLE (
    SELECT 
        t.*
    FROM query_table(table_name::VARCHAR) t
);

