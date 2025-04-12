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

CREATE OR REPLACE MACRO udfx.make_snake_case_name(name) AS (
    lower(name).regexp_replace('[\W+]', ' ', 'g').trim().regexp_replace('[\W]+', '_', 'g')
);

-- CREATE OR REPLACE MACRO udfx.normalize_name(name) AS (
--     lower(name).regexp_replace('[\W+]', ' ', 'g').trim().regexp_replace('[\W]+', '_', 'g')
-- );

-- name is prefixed by 'EQUIPMENT:'
CREATE OR REPLACE MACRO udfx.make_equiclass_name(name) AS (
    udfx.make_snake_case_name(replace(name, 'EQUIPMENT:', 'equiclass'))
); 

