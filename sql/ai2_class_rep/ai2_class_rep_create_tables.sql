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
    responsible_officer VARCHAR,
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

-- TEMP TABLES
CREATE OR REPLACE TEMP TABLE temp_signal_type(
    ai2_reference VARCHAR NOT NULL,
    signal_type VARCHAR,
    PRIMARY KEY(ai2_reference)
);


-- MACROS
CREATE OR REPLACE MACRO format_output_type(a) AS
    CASE 
        WHEN upper(a) = 'DIGITAL' THEN 'DIGITAL'
        WHEN upper(a) = 'MA' OR upper(a) = 'MV' THEN 'ANALOGUE' 
    END;



-- INSTRUMENT

-- INSTRUMENT: LSTNCO (conductive level device)
CREATE OR REPLACE TABLE ai2_class_rep.equiclass_lstnco (
    ai2_reference VARCHAR NOT NULL,
    uniclass_code VARCHAR,
    uniclass_desc VARCHAR,
    location_on_site VARCHAR,
    manufacturers_asset_life_yr INTEGER,
    ip_rating VARCHAR,
    lstn_signal_type VARCHAR,
    lstn_output_type VARCHAR,
    lstn_range_max DECIMAL(10, 3),
    lstn_range_min DECIMAL(10, 3),
    lstn_range_units VARCHAR,
    lstn_supply_voltage INTEGER,
    lstn_supply_voltage_units VARCHAR,
    PRIMARY KEY(ai2_reference)
);

-- INSTRUMENT: LSTNUT (ultrasonic time of flight level device)
CREATE OR REPLACE TABLE ai2_class_rep.equiclass_lstnut (
    ai2_reference VARCHAR NOT NULL,
    uniclass_code VARCHAR,
    uniclass_desc VARCHAR,
    location_on_site VARCHAR,
    manufacturers_asset_life_yr INTEGER,
    ip_rating VARCHAR,
    lstn_signal_type VARCHAR,
    lstn_range_max DECIMAL(10, 3),
    lstn_range_min DECIMAL(10, 3),
    lstn_range_units VARCHAR,
    lstn_relay_1_function VARCHAR,
    lstn_relay_1_off_level_m DECIMAL(10, 3),
    lstn_relay_1_on_level_m DECIMAL(10, 3),
    lstn_relay_2_function VARCHAR,
    lstn_relay_2_off_level_m DECIMAL(10, 3),
    lstn_relay_2_on_level_m DECIMAL(10, 3),
    lstn_relay_3_function VARCHAR,
    lstn_relay_3_off_level_m DECIMAL(10, 3),
    lstn_relay_3_on_level_m DECIMAL(10, 3),
    lstn_relay_4_function VARCHAR,
    lstn_relay_4_off_level_m DECIMAL(10, 3),
    lstn_relay_4_on_level_m DECIMAL(10, 3),
    lstn_relay_5_function VARCHAR,
    lstn_relay_5_off_level_m DECIMAL(10, 3),
    lstn_relay_5_on_level_m DECIMAL(10, 3),
    lstn_relay_6_function VARCHAR,
    lstn_relay_6_off_level_m DECIMAL(10, 3),
    lstn_relay_6_on_level_m DECIMAL(10, 3),
    lstn_set_to_snort VARCHAR,
    lstn_supply_voltage INTEGER,
    lstn_supply_voltage_units VARCHAR,
    lstn_transducer_model VARCHAR,
    lstn_transducer_serial_no VARCHAR,
    lstn_transmitter_model VARCHAR,
    lstn_transmitter_serial_no VARCHAR,
    PRIMARY KEY(ai2_reference)
);

-- ELECTRICAL: STARDO (direct on line starter)
CREATE OR REPLACE TABLE ai2_class_rep.equiclass_stardo (
    ai2_reference VARCHAR NOT NULL,
    uniclass_code VARCHAR,
    uniclass_desc VARCHAR,
    location_on_site VARCHAR,
    manufacturers_asset_life_yr INTEGER,
    ip_rating VARCHAR,
    star_number_of_phase VARCHAR,
    star_number_of_ways INTEGER,
    star_rated_current_a DECIMAL(10, 3),
    star_rated_power_kw DECIMAL(10, 3),
    star_rated_voltage INTEGER,
    star_rated_voltage_units VARCHAR,
    star_sld_ref_no VARCHAR,
    PRIMARY KEY(ai2_reference)
);
