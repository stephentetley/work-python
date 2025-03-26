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

CREATE TYPE checker_serverity AS ENUM ('error', 'warning', 'style-issue', 'okay');

CREATE OR REPLACE TABLE asset_checking.checking_results (
    serverity checker_serverity NOT NULL,
    category VARCHAR NOT NULL,
    checker_name VARCHAR NOT NULL,
    checker_description VARCHAR NOT NULL,
    checker_exceptions STRUCT(item VARCHAR, name VARCHAR)[],
);

CREATE OR REPLACE MACRO make_exceptions_text(exceptions) AS (
    list_transform(exceptions, st -> format(E'{}: {}', st.item, st.name)).list_aggregate('string_agg', E', ')
);

CREATE OR REPLACE VIEW asset_checking.vw_checking_report AS
SELECT 
    t.*,
    list_transform(t.checker_exceptions, st -> format(E'{}: {}', st.item, st.name)).list_aggregate('string_agg', E', ') AS exceptions_text
FROM asset_checking.checking_results t;

