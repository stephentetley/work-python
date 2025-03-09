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


CREATE OR REPLACE MACRO union_xlsx_files_with_glob(glob_string) AS TABLE
(WITH cte AS (
    SELECT 
        list('(SELECT t."Date", t."Asset Ref", t."AssetName", t."Status" FROM read_xlsx(''' || file::VARCHAR || ''', header = true) AS t)') AS file_names,
    FROM glob(glob_string::VARCHAR)
)
SELECT 
    concat_ws(E'\n',
       'INSERT INTO soev_rawdata.aide_exports BY NAME',
        list_aggregate(t.file_names, 'string_agg', E'\nUNION\n'),
        ';'
        ) AS sql_text
FROM cte t)
;

PREPARE load_aide_changes AS
SELECT sql_text FROM union_xlsx_files_with_glob( $globpath );

