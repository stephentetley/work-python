"""
Copyright 2023 Stephen Tetley

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

duckdb_ddl = """
-- duckdb create tables

CREATE OR REPLACE TABLE telemetry_facts(
    outstation_name TEXT NOT NULL, 
    site_sai_num TEXT,
    site_name TEXT,
    outstation_comment TEXT,
    outstation_id TEXT,
    outstation_pli_num TEXT,
    PRIMARY KEY(outstation_name)
);

-- No primary key, `asset_id` might not be unique
CREATE OR REPLACE TABLE worklist(
    asset_id TEXT NOT NULL,
    submit_timestamp TIMESTAMP NOT NULL, 
    asset_name TEXT NOT NULL,
    status TEXT
);

CREATE OR REPLACE TABLE s4_equipment_master(
    equi_id TEXT NOT NULL,
    equi_name TEXT NOT NULL,
    func_loc TEXT,
    super_id TEXT,
    tag_name TEXT,
    asset_status TEXT,
    object_type TEXT,
    manufacturer TEXT,
    model TEXT,
    specific_model TEXT,
    serial_number TEXT,
    startup_date TIMESTAMP,
    equi_category TEXT,
    PRIMARY KEY(equi_id)
);

CREATE OR REPLACE TABLE aib_equipment_master(
    pli_num TEXT NOT NULL,
    common_name TEXT NOT NULL,
    installed_from TIMESTAMP,
    manufacturer TEXT,
    model TEXT,
    specific_model TEXT,
    serial_number TEXT,
    asset_status TEXT,
    PRIMARY KEY(pli_num)
);

"""
