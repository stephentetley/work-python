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


CREATE SCHEMA IF NOT EXISTS s4_class_rep_staging;

CREATE OR REPLACE TABLE s4_class_rep_staging.floc_ai2_sai_references (
    floc_id VARCHAR,
    ai2_aib_references VARCHAR[],
    PRIMARY KEY(floc_id)
);


CREATE OR REPLACE TABLE s4_class_rep_staging.floc_s4_aib_reference (
    floc_id VARCHAR,
    s4_aib_reference VARCHAR,
    PRIMARY KEY(floc_id)
);

-- ## AIB_REFERENCE (floc)


CREATE OR REPLACE TABLE s4_class_rep.floc_aib_reference (
    floc_id VARCHAR NOT NULL,
    ai2_aib_references VARCHAR[],
    s4_aib_reference VARCHAR,
    PRIMARY KEY(floc_id)
);

CREATE OR REPLACE TABLE s4_class_rep_staging.floc_ai2_sai_references (
    floc_id VARCHAR,
    ai2_aib_references VARCHAR[],
    PRIMARY KEY(floc_id)
);




-- ## AIB_REFERENCE (equi)

-- equi AIB_REFERENCE uses staging tables
-- We consider equipment as having principal SAI and PLI numbers. 
-- Equipment may also have a list of extra refernces which probably 
-- indicate data errors...

CREATE OR REPLACE TABLE s4_class_rep_staging.equi_ai2_sai_reference (
    equipment_id VARCHAR,
    value_index INTEGER,
    ai2_sai_reference VARCHAR,
    PRIMARY KEY(equipment_id)
);

CREATE OR REPLACE TABLE s4_class_rep_staging.equi_ai2_pli_reference (
    equipment_id VARCHAR,
    value_index INTEGER,
    ai2_pli_reference VARCHAR,
    PRIMARY KEY(equipment_id)
);


CREATE OR REPLACE VIEW s4_class_rep_staging.vw_equi_ai2_extra_references AS 
SELECT
    e.equipment_id AS equipment_id,
    array_agg(eav.atwrt) AS extra_aib_references,
FROM s4_class_rep.equi_master_data e
JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
ANTI JOIN s4_class_rep_staging.equi_ai2_sai_reference sai ON eav.atwrt = sai.ai2_sai_reference 
ANTI JOIN s4_class_rep_staging.equi_ai2_pli_reference pli ON eav.atwrt = pli.ai2_pli_reference 
WHERE eav.charid = 'AI2_AIB_REFERENCE'
GROUP BY equipment_id;

CREATE OR REPLACE VIEW s4_class_rep_staging.vw_equi_s4_aib_references AS
SELECT DISTINCT ON(e.equipment_id)
    e.equipment_id AS equipment_id,
    any_value(CASE WHEN eav.charid = 'S4_AIB_REFERENCE' THEN eav.atwrt ELSE NULL END) AS s4_aib_reference,
FROM s4_class_rep.equi_master_data e
JOIN s4_fd_raw_data.valuaequi_valuaequi1 eav ON eav.equi = e.equipment_id
GROUP BY equipment_id;


