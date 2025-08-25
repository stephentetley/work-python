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


CREATE OR REPLACE TEMPORARY MACRO empty_string_to_null(str) AS
    IF(str='', null, str);

CREATE OR REPLACE TEMPORARY MACRO rename_class(str) AS
    IF(str LIKE '00_ %', str[5:], str);


CREATE OR REPLACE TEMPORARY TABLE classlist_landing (
    row_ix BIGINT,
    class_name  VARCHAR,
    char_name VARCHAR,
    enum_value  VARCHAR,
    descrip VARCHAR,
    datatype VARCHAR,
    datatype_length INTEGER,
    datatype_precision INTEGER,
);


CREATE OR REPLACE TEMPORARY TABLE classlist_normed (
    ix INTEGER,
    row_type INTEGER,
    class_name VARCHAR,
    char_name VARCHAR,
    enum_value VARCHAR,
    descrip VARCHAR,
    datatype VARCHAR,
    datatype_length INTEGER,
    datatype_precision INTEGER,
);


CREATE OR REPLACE MACRO read_classlist_export(xlsx_file) AS TABLE
WITH trimmed AS (
    SELECT 
        row_number() OVER () AS row_ix_trimmed,
        empty_string_to_null(rename_class(trim(t."B1"))) AS class_name,
        empty_string_to_null(trim(t."C1")) AS char_name,
        empty_string_to_null(trim(t."D1")) AS enum_value,
        empty_string_to_null(trim(t."E1")) AS descrip,
        empty_string_to_null(trim(t."G1")) AS datatype,
        empty_string_to_null(trim(t."H1")) AS datatype_length,
        empty_string_to_null(trim(t."I1")) AS datatype_precision,
    FROM read_xlsx(xlsx_file :: VARCHAR, all_varchar=true, header=false) AS t
), filtered AS (
    SELECT 
        row_number() OVER () AS row_ix,
        * EXCLUDE(row_ix_trimmed)
    
    FROM trimmed 
    WHERE class_name IS NOT NULL OR char_name IS NOT NULL OR enum_value IS NOT NULL
    ORDER BY row_ix_trimmed
)
SELECT * FROM filtered;

CREATE OR REPLACE TEMPORARY MACRO normalize_landing_data(start_row) AS TABLE (
WITH RECURSIVE cte(ix, row_type, class_name, char_name, enum_value, descrip, datatype, datatype_length, datatype_precision) USING KEY (ix) AS (
        SELECT 1, 1, t.class_name, t.char_name, t.enum_value, t.descrip, t.datatype, t.datatype_length, t.datatype_precision 
        FROM classlist_landing t
        WHERE row_ix = start_row
    UNION
        SELECT 
            classlist_landing.row_ix, 
            CASE 
                WHEN classlist_landing.class_name IS NOT NULL THEN 1 
                WHEN classlist_landing.char_name IS NOT NULL THEN 2 
                WHEN classlist_landing.enum_value IS NOT NULL THEN 3 
                ELSE 0
            END AS row_type1,
            CASE 
                WHEN row_type1 = 1 THEN classlist_landing.class_name
                ELSE cte.class_name
            END,
            CASE
                WHEN row_type1 = 1 THEN null
                WHEN row_type1 = 2 THEN classlist_landing.char_name
                WHEN row_type1 = 3 THEN cte.char_name
            END,
            CASE
                WHEN row_type1 = 1 THEN null
                WHEN row_type1 = 2 THEN null
                WHEN row_type1 = 3 THEN classlist_landing.enum_value
            END,
            classlist_landing.descrip,
            CASE
                WHEN row_type1 = 2 THEN classlist_landing.datatype
                WHEN row_type1 = 3 THEN 'ENUM'
                ELSE null
            END,
            classlist_landing.datatype_length,
            classlist_landing.datatype_precision,
        FROM cte, classlist_landing
        WHERE classlist_landing.row_ix = cte.ix + 1
)
SELECT * FROM cte);


PREPARE load_classlist AS
INSERT INTO classlist_landing BY NAME
SELECT * FROM read_classlist_export($1)
;

PREPARE delete_classlist_landing AS
DELETE FROM classlist_landing
;


PREPARE norm_classlist AS 
INSERT INTO classlist_normed BY NAME (
SELECT * FROM normalize_landing_data($1)
);

PREPARE delete_classlist_normed AS
DELETE FROM classlist_normed
;


PREPARE fill_equi_characteristics AS 
INSERT OR REPLACE INTO s4_classlists.equi_characteristics BY NAME (
    SELECT DISTINCT ON (class_name, char_name)
        class_name AS class_name,
        char_name AS char_name,
        descrip AS class_description,
        datatype AS char_type,
        datatype_length AS char_length,
        datatype_precision AS char_precision,
    FROM classlist_normed t
    WHERE t.class_name IS NOT NULL AND t.char_name IS NOT NULL
    AND $1 = 1
);

PREPARE fill_floc_characteristics AS 
INSERT INTO s4_classlists.floc_characteristics BY NAME (
    SELECT DISTINCT ON (class_name, char_name)
        class_name,
        char_name,
        descrip AS class_description,
        datatype AS char_type,
        datatype_length AS char_length,
        datatype_precision AS char_precision,
    FROM classlist_normed
    WHERE class_name IS NOT NULL AND char_name IS NOT NULL
    AND $1 = 1
);


PREPARE fill_equi_enums AS 
INSERT INTO s4_classlists.equi_enums BY NAME (
    SELECT 
        t.class_name AS class_name,
        t.char_name AS char_name,
        t.enum_value AS enum_value,
        t.descrip AS enum_description,
    FROM classlist_normed t
    WHERE 
        t.datatype = 'ENUM'
    AND t.enum_value IS NOT NULL
    AND $1 = 1
    ORDER BY class_name, char_name
);

PREPARE fill_floc_enums AS 
INSERT INTO s4_classlists.floc_enums BY NAME (
    SELECT 
        t.class_name AS class_name,
        t.char_name AS char_name,
        t.enum_value AS enum_value,
        t.descrip AS enum_description,
    FROM classlist_normed t
    WHERE 
        t.datatype = 'ENUM'
    AND t.enum_value IS NOT NULL
    AND $1 = 1
    ORDER BY class_name, char_name
);


-- To run, eval file then run prepared statements...
-- EXECUTE load_classlist('/home/stephen/_working/work/2025/asset_data_facts/s4_classlists/002_equi_classlist.20250822.xlsx');
-- EXECUTE norm_classlist(1);
-- EXECUTE fill_equi_characteristics(1);
-- EXECUTE fill_equi_enums(1);
-- EXECUTE delete_classlist_landing;
-- EXECUTE delete_classlist_normed;
-- EXECUTE load_classlist('/home/stephen/_working/work/2025/asset_data_facts/s4_classlists/003_floc_classlist.20250822.xlsx');
-- EXECUTE norm_classlist(1);
-- EXECUTE fill_floc_characteristics(1);
-- EXECUTE fill_floc_enums(1);
