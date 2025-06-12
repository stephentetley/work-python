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

CREATE SCHEMA IF NOT EXISTS floc_delta;

CREATE OR REPLACE TABLE floc_delta.worklist(
    requested_floc VARCHAR NOT NULL,
    floc_description VARCHAR,
    object_type VARCHAR,
    class_type VARCHAR,
    user_status VARCHAR,
    level5_system_name VARCHAR,
    easting INTEGER,
    northing INTEGER,
    solution_id VARCHAR,
    PRIMARY KEY (requested_floc)
);


CREATE OR REPLACE TABLE floc_delta.existing_flocs (
    funcloc VARCHAR NOT NULL,
    floc_name VARCHAR,
    floc_category INTEGER,
    user_status VARCHAR,
    startup_date DATE,
    cost_center INTEGER,
    maint_work_center VARCHAR,
    maintenance_plant VARCHAR,
    plant_section VARCHAR,
    easting INTEGER,
    northing INTEGER,
    PRIMARY KEY (funcloc)
);
    

CREATE OR REPLACE TABLE floc_delta.existing_and_new_flocs (
    funcloc VARCHAR,
    floc_name VARCHAR,
    floc_category INTEGER,
    user_status VARCHAR,
    floc_type VARCHAR,
    floc_class VARCHAR,
    parent_floc VARCHAR,
    easting INTEGER,
    northing INTEGER,
    solution_id VARCHAR,
    level5_system_name VARCHAR,
    PRIMARY KEY (funcloc)
    );


CREATE OR REPLACE TABLE floc_delta.new_generated_flocs(
    funcloc VARCHAR NOT NULL,
    floc_name VARCHAR,
    floc_category INTEGER,
    user_status VARCHAR,
    floc_type VARCHAR,
    floc_class VARCHAR,
    parent_floc VARCHAR,
    easting INTEGER,
    northing INTEGER,
    solution_id VARCHAR,
    level5_system_name VARCHAR,
    PRIMARY KEY (funcloc)
    );

CREATE OR REPLACE VIEW floc_delta.vw_existing_ancestor AS 
WITH cte AS (
SELECT 
    t.funcloc AS gen_funcloc, 
    t1.funcloc AS ancestor,
FROM floc_delta.new_generated_flocs t
JOIN floc_delta.existing_flocs t1 ON  t.funcloc ^@ t1.funcloc 
) 
SELECT gen_funcloc AS new_funcloc, max(ancestor) AS existing_ancestor
FROM cte
GROUP BY gen_funcloc;

CREATE OR REPLACE VIEW floc_delta.vw_new_flocs AS 
SELECT 
    t.* EXCLUDE(easting, northing, user_status),
    t1.existing_ancestor AS existing_ancestor,
    t2.startup_date AS startup_date,
    t2.cost_center AS cost_center,
    t2.maintenance_plant AS maintenance_plant,
    IF (t.floc_category = 4, t.floc_type, NULL) AS plant_section,
    IF (t.user_status IS NULL, t2.user_status, t.user_status) AS user_status,
    IF (t.easting IS NULL, t2.easting, t.easting) AS easting,
    IF (t.northing IS NULL, t2.northing, t.northing) AS northing,
FROM floc_delta.new_generated_flocs t
LEFT OUTER JOIN floc_delta.vw_existing_ancestor t1 ON t1.new_funcloc = t.funcloc
LEFT OUTER JOIN floc_delta.existing_flocs t2 ON t2.funcloc = t1.existing_ancestor;

-- NOTE list(plant_uml1).... is not providing a sufficient ordering
CREATE OR REPLACE VIEW floc_delta.vw_plant_uml_export AS
WITH cte1 AS (
    (SELECT 
        t.funcloc AS functloc,
        ' ' || repeat('+', t.floc_category) || ' ' || t.funcloc || ' | ' || t.floc_name AS plant_uml1, 
    FROM floc_delta.existing_flocs t)
    UNION
    (SELECT 
        t.funcloc AS functloc,
        ' ' || repeat('+', t.floc_category) || ' <color:Green>' || t.funcloc || ' | <color:Green>' || t.floc_name AS plant_uml1,  
    FROM floc_delta.new_generated_flocs t)
), cte2 AS (
    SELECT functloc, plant_uml1 FROM cte1 ORDER BY functloc 
)
SELECT 
    concat_ws(E'\n',
        '@startsalt',
        '{',
        '{T',
        ' +Functional Location | Description',
        string_agg(plant_uml1, E'\n' ORDER BY functloc ASC),
        '}',
        '}',
        '@endsalt'
        ) AS plant_uml,
FROM cte2
;

