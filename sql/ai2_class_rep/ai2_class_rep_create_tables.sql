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

CREATE SCHEMA IF NOT EXISTS ai2_class_rep;

CREATE OR REPLACE TABLE ai2_class_rep.equi_master_data (
    ai2_reference VARCHAR NOT NULL,
    common_name VARCHAR NOT NULL,
    equipment_name VARCHAR,
    equipment_type VARCHAR,
    installed_from DATE,
    manufacturer VARCHAR,
    model VARCHAR,
    specific_model_frame VARCHAR,
    serial_number VARCHAR,
    asset_status VARCHAR,
    p_and_i_tag VARCHAR,
    weight_kg INTEGER,
    work_centre VARCHAR,
    PRIMARY KEY(ai2_reference)
);

CREATE OR REPLACE TABLE ai2_class_rep.equi_memo_text(
    ai2_reference VARCHAR NOT NULL,
    memo_line1 VARCHAR,
    memo_line2 VARCHAR,
    memo_line3 VARCHAR,
    memo_line4 VARCHAR,
    memo_line5 VARCHAR,
    PRIMARY KEY(ai2_reference)
);

CREATE OR REPLACE TABLE ai2_class_rep.equiclass_east_north (
    ai2_reference VARCHAR NOT NULL,
    grid_ref VARCHAR,
    easting INTEGER,
    northing INTEGER,
    PRIMARY KEY(ai2_reference)
);

CREATE OR REPLACE TABLE ai2_class_rep.equiclass_asset_condition (
    ai2_reference VARCHAR NOT NULL,
    condition_grade VARCHAR,
    condition_grade_reason VARCHAR,
    survey_date INTEGER,
    PRIMARY KEY(ai2_reference)
);
