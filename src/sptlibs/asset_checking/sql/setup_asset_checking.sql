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

CREATE SCHEMA IF NOT EXISTS asset_checking;

CREATE TYPE checker_severity AS ENUM ('error', 'warning', 'style-issue', 'okay');

CREATE OR REPLACE TABLE asset_checking.checking_results (
    severity checker_severity NOT NULL,
    category VARCHAR NOT NULL,
    checker_name VARCHAR NOT NULL,
    checker_description VARCHAR NOT NULL,
    item_id VARCHAR, 
    item_name VARCHAR,
);


CREATE OR REPLACE VIEW asset_checking.vw_checking_report AS
WITH cte AS ( 
    SELECT 
        list(struct_pack(item := item_id, name := item_name)) AS checker_exceptions, 
        severity, 
        category, 
        checker_name, 
        checker_description,
    FROM asset_checking.checking_results
    GROUP BY severity, category, checker_name, checker_description
)
SELECT 
    t.* EXCLUDE(checker_exceptions),
    list_transform(t.checker_exceptions, st -> format(E'{}: {}', st.item, st.name)).list_aggregate('string_agg', E', ') AS exceptions_text
FROM cte t;


CREATE OR REPLACE MACRO checker_classification(severity, category, checker_name, checker_descr) AS TABLE (
SELECT 
   severity::checker_severity AS severity,
   category::VARCHAR AS category,
   checker_name::VARCHAR AS checker_name,
   checker_descr::VARCHAR AS checker_description
);
