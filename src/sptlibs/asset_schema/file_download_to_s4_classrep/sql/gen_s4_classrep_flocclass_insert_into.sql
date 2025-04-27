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

-- INSERT INTO `flocclass_` tables

-- TODO
-- This is file_download specific and should in a file_download specific package


WITH cte AS (
    SELECT 
        fch.class_name AS class_name,
        array_agg(format(E'    any_value(CASE WHEN eav.charid = ''{}'' THEN {} ELSE NULL END) AS {},', 
                    fch.char_name,
                    CASE 
                        WHEN fch.refined_char_type = 'DECIMAL' THEN 'TRY_CAST(eav.atwrt AS DECIMAL)' 
                        WHEN fch.refined_char_type = 'INTEGER' THEN 'round(eav.atflv, 0)' 
                        ELSE 'eav.atwrt'
                    END,                    
                    lower(fch.char_name))) AS field_elements,
    FROM s4_classlists.vw_refined_floc_characteristic_defs fch
    JOIN s4_classlists.vw_floc_class_defs fcl ON fcl.class_name = fch.class_name 
    WHERE fcl.is_system_class = true
    GROUP BY fch.class_name
)
SELECT 
    t.class_name AS class_name,
    concat_ws(E'\n',
        format('INSERT OR REPLACE INTO s4_classrep.flocclass_{} BY NAME', lower(t.class_name)),
        'SELECT DISTINCT ON(f.funcloc_id)', 
        '    f.funcloc_id AS funcloc_id,',
        list_sort(t.field_elements).list_aggregate('string_agg', E'\n'),
        'FROM s4_classrep.floc_masterdata f',
        'JOIN file_download.classfloc clz ON clz.funcloc = f.funcloc_id',
        'JOIN file_download.valuafloc eav ON eav.funcloc = f.funcloc_id',
        format('WHERE clz.classname = ''{}''', t.class_name),
        'GROUP BY f.funcloc_id;') AS sql_text,
FROM cte t
ORDER BY t.class_name ASC;