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

CREATE SCHEMA IF NOT EXISTS aide_changes;

CREATE OR REPLACE TABLE aide_changes.ih08_equi (
    equi_id VARCHAR NOT NULL,
    aide_change VARCHAR,
    description VARCHAR,
    equi_type VARCHAR,
    functional_location VARCHAR,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    asset_status VARCHAR,
    sai_number VARCHAR,
    pli_number VARCHAR,
    PRIMARY KEY (equi_id)
);

CREATE OR REPLACE TABLE aide_changes.ai2_equipment_changes (
    ai2_ref VARCHAR NOT NULL,
    aide_change VARCHAR,
    equi_name VARCHAR,
    ai2_equi_type VARCHAR,
    to_be_translated BOOLEAN,
    s4_category VARCHAR,
    s4_object_type VARCHAR,
    s4_class VARCHAR,
    common_name VARCHAR,
    installed_from TIMESTAMP_MS,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    asset_status VARCHAR,
    grid_ref VARCHAR,
    in_aide BOOLEAN,
    pandi_tag VARCHAR,
    PRIMARY KEY (ai2_ref)
);


-- ai2 not synced
CREATE OR REPLACE VIEW aide_changes.vw_ai2_not_synced AS
SELECT t.* EXCLUDE (aide_change, in_aide, to_be_translated),
FROM aide_changes.ai2_equipment_changes t
ANTI JOIN aide_changes.ih08_equi t1 ON t1.pli_number = t.ai2_ref
WHERE t.aide_change <> 'Child New'
AND t.to_be_translated = true
;

-- s4 not synced
CREATE OR REPLACE VIEW aide_changes.vw_s4_not_synced AS
SELECT t.* EXCLUDE (aide_change),
FROM aide_changes.ih08_equi t
WHERE t.pli_number IS NULL
;


-- s4 new (may include not synced items)
CREATE OR REPLACE VIEW aide_changes.vw_s4_new AS
SELECT 
    t.* EXCLUDE (aide_change, in_aide, grid_ref, to_be_translated), 
    t.grid_ref AS osgb,
    t.ai2_ref AS ai2_pli_reference,
    t1.sai_num AS ai2_sai_reference,
    t2.easting AS easting,
    t2.northing AS northing,
FROM aide_changes.ai2_equipment_changes t
LEFT JOIN raw_data.vw_ai2_parent_sai_nums t1 ON t1.pli_num = t.ai2_ref 
CROSS JOIN udfx.get_east_north(t.grid_ref) t2
WHERE t.aide_change = 'Child New'
AND t.to_be_translated = true
;

-- s3 changes
CREATE OR REPLACE VIEW aide_changes.vw_s4_changes AS
SELECT 
    t1.equi_id AS equi_id,
    t.* EXCLUDE (in_aide, grid_ref, to_be_translated),
    t.grid_ref AS osgb,
    t2.easting AS easting,
    t2.northing AS northing,
FROM aide_changes.ai2_equipment_changes t
LEFT JOIN aide_changes.ih08_equi t1 ON t1.pli_number = t.ai2_ref
CROSS JOIN udfx.get_east_north(t.grid_ref) t2
WHERE t.aide_change IN('Edit Relationship', 'Child Deleted')
AND t.to_be_translated = true
;
