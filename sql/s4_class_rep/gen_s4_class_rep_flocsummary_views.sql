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


WITH cte AS (
    SELECT 
        lower(fcl.class_name) AS class_name,
    FROM s4_classlists.vw_floc_class_defs fcl 
    WHERE fcl.is_system_class = true
)
SELECT
    t.class_name AS class_name,
    concat_ws(E'\n',
        format('CREATE OR REPLACE VIEW s4_class_rep.vw_flocsummary_{} AS', t.class_name),
        'SELECT', 
        '        fmd.floc_id AS floc_id,',
        '        fmd.functional_location AS functional_location,', 
        '        fmd.description AS description,',
        '        fmd.startup_date AS startup_date,',
        '        fmd.object_type AS object_type,',
        '        fmd.user_status AS user_status,',
        '    fc.* EXCLUDE (floc_id),',
        format('FROM s4_class_rep.flocclass_{} fc', t.class_name), 
        'JOIN s4_class_rep.floc_master_data fmd ON fmd.floc_id = fc.floc_id;' ) AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;