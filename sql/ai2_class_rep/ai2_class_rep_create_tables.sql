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



-- ## MACROS

CREATE OR REPLACE MACRO ai2_class_rep.format_signal(signal_min, signal_max, signal_unit) AS (
    signal_min || ' - ' || signal_max || ' ' || upper(signal_unit)
);

CREATE OR REPLACE MACRO ai2_class_rep.format_output_type(a) AS (
    CASE 
        WHEN upper(a) = 'DIGITAL' THEN 'DIGITAL'
        WHEN upper(a) = 'MA' OR upper(a) = 'MV' THEN 'ANALOGUE' 
    END
);


CREATE OR REPLACE MACRO ai2_class_rep.voltage_ac_or_dc(ac_or_dc) AS (
    CASE 
        WHEN upper(ac_or_dc) = 'DIRECT CURRENT' THEN 'VDC' 
        WHEN upper(ac_or_dc) = 'ALTERNATING CURRENT' THEN 'VAC'
        ELSE NULL
    END
);


CREATE OR REPLACE MACRO ai2_class_rep.size_to_millimetres(size_units, size_value) AS (
    CASE 
        WHEN upper(size_units) = 'MILLIMETRES' THEN round(size_value, 0)
        WHEN upper(size_units) = 'CENTIMETRES' THEN round(size_value  * 10, 0) 
        WHEN upper(size_units) = 'INCH' THEN round(size_value * 25.4, 0) 
        ELSE NULL
    END
);

-- Ignore `KILOVOLT AMP`
CREATE OR REPLACE MACRO ai2_class_rep.power_to_killowatts(power_units, power_value) AS (
    CASE 
        WHEN upper(power_units) = 'KILOWATTS' THEN power_value
        WHEN upper(power_units) = 'WATTS' THEN power_value  * 1000.0  
        ELSE NULL
    END
);


-- ## TABLES

CREATE OR REPLACE TABLE ai2_class_rep.equi_master_data (
    equipment_key UBIGINT NOT NULL,
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
    responsible_officer VARCHAR,
    PRIMARY KEY(equipment_key)
);

CREATE OR REPLACE TABLE ai2_class_rep.equi_memo_text(
    equipment_key UBIGINT NOT NULL,
    ai2_reference VARCHAR NOT NULL,
    memo_line1 VARCHAR,
    memo_line2 VARCHAR,
    memo_line3 VARCHAR,
    memo_line4 VARCHAR,
    memo_line5 VARCHAR,
    PRIMARY KEY(ai2_reference)
);

CREATE OR REPLACE TABLE ai2_class_rep.equi_east_north (
    equipment_key UBIGINT NOT NULL,
    ai2_reference VARCHAR NOT NULL,
    grid_ref VARCHAR,
    easting INTEGER,
    northing INTEGER,
    PRIMARY KEY(equipment_key)
);

CREATE OR REPLACE TABLE ai2_class_rep.equi_asset_condition (
    equipment_key UBIGINT NOT NULL,
    ai2_reference VARCHAR NOT NULL,
    condition_grade VARCHAR,
    condition_grade_reason VARCHAR,
    survey_date INTEGER,
    PRIMARY KEY(equipment_key)
);


