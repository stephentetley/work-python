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


-- Setup classlist info tables...


CREATE SCHEMA IF NOT EXISTS s4_classlists;

CREATE OR REPLACE TABLE s4_classlists.floc_characteristics (
    class_name TEXT NOT NULL,
    char_name TEXT NOT NULL,
    class_description TEXT,
    char_description TEXT,
    char_type TEXT,
    char_length INTEGER,
    char_precision INTEGER,
    PRIMARY KEY(class_name, char_name)
);


CREATE OR REPLACE TABLE s4_classlists.equi_characteristics (
    class_name TEXT NOT NULL,
    char_name TEXT NOT NULL,
    class_description TEXT,
    char_description TEXT,
    char_type TEXT,
    char_length INTEGER,
    char_precision INTEGER,
    PRIMARY KEY(class_name, char_name)
);


-- Dont bother with primary key as it is a 3-tuple.
CREATE OR REPLACE TABLE s4_classlists.floc_enums (
    class_name TEXT NOT NULL,
    char_name TEXT NOT NULL,
    enum_value TEXT NOT NULL,
    enum_description TEXT,
    PRIMARY KEY(class_name, char_name, enum_value)
);


-- Dont bother with primary key as it is a 3-tuple.
CREATE OR REPLACE TABLE s4_classlists.equi_enums (
    class_name TEXT NOT NULL,
    char_name TEXT NOT NULL,
    enum_value TEXT NOT NULL,
    enum_description TEXT,
    PRIMARY KEY(class_name, char_name, enum_value)
);



CREATE OR REPLACE VIEW s4_classlists.vw_refined_equi_characteristic_defs AS
WITH cte1 AS (
    (SELECT 
        t.class_name AS class_name,
        t.char_name AS char_name, 
        true AS is_enum,
    FROM s4_classlists.equi_characteristics t
    SEMI JOIN s4_classlists.equi_enums t1 ON t1.class_name = t.class_name AND t1.char_name = t.char_name)
UNION
    (SELECT 
        t.class_name AS class_name,
        t.char_name AS char_name, 
        false AS is_enum,
    FROM s4_classlists.equi_characteristics t
    ANTI JOIN s4_classlists.equi_enums t1 ON t1.class_name = t.class_name AND t1.char_name = t.char_name)
)
SELECT 
    t.class_name AS class_name,
    t.char_name AS char_name, 
    t.class_description AS class_description,
    t.char_description AS char_description,
    t.char_type AS s4_char_type,
    t.char_length AS char_len,
    t.char_precision AS char_precision,
    CASE 
        WHEN t.char_type = 'CHAR' THEN 'TEXT'
        WHEN t.char_type = 'NUM' AND (t.char_precision IS NULL OR t.char_precision = 0) AND t.char_length < 10 THEN 'INTEGER'
        WHEN t.char_type = 'NUM' AND (t.char_precision IS NULL OR t.char_precision = 0) AND t.char_length >= 10 AND t.char_length < 19 THEN 'BIGINT'
        WHEN t.char_type = 'NUM' AND (t.char_precision IS NULL OR t.char_precision = 0) AND t.char_length >= 19 THEN 'HUGEINT'
        WHEN t.char_type = 'NUM' AND t.char_precision > 0 THEN 'DECIMAL'
        ELSE t.char_type
    END AS refined_char_type,
    CASE 
        WHEN t.char_type = 'CHAR' THEN 'VARCHAR'
        WHEN t.char_type = 'NUM' AND (t.char_precision IS NULL OR t.char_precision = 0) AND t.char_length < 10 THEN 'INTEGER'
        WHEN t.char_type = 'NUM' AND (t.char_precision IS NULL OR t.char_precision = 0) AND t.char_length >= 10 AND t.char_length < 19 THEN 'BIGINT'
        WHEN t.char_type = 'NUM' AND (t.char_precision IS NULL OR t.char_precision = 0) AND t.char_length >= 19 THEN 'HUGEINT'
        WHEN t.char_type = 'NUM' AND t.char_precision > 0 THEN format('DECIMAL({}, {})', t.char_length, t.char_precision)
        ELSE t.char_type
    END AS ddl_data_type,
    t1.is_enum AS is_enum,
FROM s4_classlists.equi_characteristics t
JOIN cte1 t1 ON t1.class_name = t.class_name AND t1.char_name = t.char_name;

CREATE OR REPLACE VIEW s4_classlists.vw_refined_floc_characteristic_defs AS
WITH cte1 AS (
    (SELECT 
        t.class_name AS class_name,
        t.char_name AS char_name, 
        true AS is_enum,
    FROM s4_classlists.floc_characteristics t
    SEMI JOIN s4_classlists.floc_enums t1 ON t1.class_name = t.class_name AND t1.char_name = t.char_name)
UNION
    (SELECT 
        t.class_name AS class_name,
        t.char_name AS char_name, 
        false AS is_enum,
    FROM s4_classlists.floc_characteristics t
    ANTI JOIN s4_classlists.floc_enums t1 ON t1.class_name = t.class_name AND t1.char_name = t.char_name)
)
SELECT 
    t.class_name AS class_name,
    t.char_name AS char_name, 
    t.class_description AS class_description,
    t.char_description AS char_description,
    t.char_type AS s4_char_type,
    t.char_length AS char_len,
    t.char_precision AS char_precision,
    CASE 
        WHEN t.char_type = 'CHAR' THEN 'TEXT'
        WHEN t.char_type = 'NUM' AND (t.char_precision IS NULL OR t.char_precision = 0) THEN 'INTEGER'
        WHEN t.char_type = 'NUM' AND t.char_precision > 0 THEN 'DECIMAL'
        ELSE t.char_type
    END AS refined_char_type,
    CASE 
        WHEN t.char_type = 'CHAR' THEN 'VARCHAR'
        WHEN t.char_type = 'NUM' AND (t.char_precision IS NULL OR t.char_precision = 0) THEN 'INTEGER'
        WHEN t.char_type = 'NUM' AND t.char_precision > 0 THEN format('DECIMAL({}, {})', t.char_length, t.char_precision)
        ELSE t.char_type
    END AS ddl_data_type,
    t1.is_enum AS is_enum,
FROM s4_classlists.floc_characteristics t
JOIN cte1 t1 ON t1.class_name = t.class_name AND t1.char_name = t.char_name;


CREATE OR REPLACE VIEW s4_classlists.vw_equi_class_defs (class_name, class_description, is_object_class) AS
WITH cte_equi_classes AS (
    SELECT
        DISTINCT ON(ec.class_name)
        ec.class_name AS class_name,
        ec.class_description AS class_description,
    FROM
        s4_classlists.equi_characteristics AS ec
), 
cte_has_uniclass_code  AS (
    SELECT ec.class_name AS class_name,
    FROM s4_classlists.equi_characteristics ec
    WHERE ec.char_name = 'UNICLASS_CODE'
)
SELECT * FROM (
    (SELECT 
        base.class_name AS class_name,
        base.class_description AS class_description,
        TRUE AS is_object_class,
    FROM cte_equi_classes base
    JOIN cte_has_uniclass_code has_uniclass
        ON has_uniclass.class_name = base.class_name)
    UNION
    (SELECT 
        base.class_name AS class_name,
        base.class_description AS class_description,
        FALSE AS is_object_class,
    FROM cte_equi_classes base
    ANTI JOIN cte_has_uniclass_code has_uniclass
        ON has_uniclass.class_name = base.class_name)
);

CREATE OR REPLACE VIEW s4_classlists.vw_floc_class_defs (class_name, class_description, is_system_class) AS
WITH cte_equi_classes AS (
    SELECT
        DISTINCT ON(fc.class_name)
        fc.class_name AS class_name,
        fc.class_description AS class_description,
    FROM
        s4_classlists.floc_characteristics AS fc
), 
cte_has_uniclass_code  AS (
    SELECT fc.class_name AS class_name,
    FROM s4_classlists.floc_characteristics fc
    WHERE fc.char_name = 'SYSTEM_TYPE'
)
SELECT * FROM (
    (SELECT 
        base.class_name AS class_name,
        base.class_description AS class_description,
        TRUE AS is_system_class,
    FROM cte_equi_classes base
    JOIN cte_has_uniclass_code has_uniclass
        ON has_uniclass.class_name = base.class_name)
    UNION
    (SELECT 
        base.class_name AS class_name,
        base.class_description AS class_description,
        FALSE AS is_system_class,
    FROM cte_equi_classes base
    ANTI JOIN cte_has_uniclass_code has_uniclass
        ON has_uniclass.class_name = base.class_name)
);

