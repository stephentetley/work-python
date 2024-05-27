"""
Copyright 2024 Stephen Tetley

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

import duckdb

def setup_tables(*, con: duckdb.DuckDBPyConnection) -> None:
    con.execute('CREATE SCHEMA IF NOT EXISTS s4_class_rep;')
    con.execute(floc_master_data_ddl)
    con.execute(equi_master_data_ddl)
    con.execute(equi_memo_text_ddl)
    con.execute(equi_east_north_ddl)
    con.execute(equi_asset_condition_ddl)


floc_master_data_ddl = """
    CREATE OR REPLACE TABLE s4_class_rep.floc_master_data (
        floc_id VARCHAR NOT NULL,
        functional_location VARCHAR NOT NULL,
        description VARCHAR,
        object_type VARCHAR,
        structure_indicator VARCHAR,
        superior_funct_loc VARCHAR,
        category VARCHAR,
        user_status VARCHAR,
        system_status VARCHAR,
        installation_allowed BOOLEAN,
        startup_date DATE,
        construction_month INTEGER,
        construction_year INTEGER,
        display_position INTEGER,
        catalog_profile VARCHAR,
        company_code INTEGER,
        cost_center INTEGER,
        controlling_area INTEGER,
        maintenance_plant INTEGER,
        main_work_center VARCHAR,
        work_center VARCHAR,
        planning_plant INTEGER,
        plant_section VARCHAR,
        object_number VARCHAR,
        location VARCHAR,
        address_ref INTEGER,
        PRIMARY KEY(floc_id)
    );
"""

equi_master_data_ddl = """
    CREATE OR REPLACE TABLE s4_class_rep.equi_master_data (
        equipment_id VARCHAR NOT NULL,
        description VARCHAR NOT NULL,
        functional_location VARCHAR,
        superord_id VARCHAR,
        category VARCHAR,
        object_type VARCHAR,
        user_status VARCHAR,
        system_status VARCHAR,
        startup_date DATE,
        construction_month INTEGER,
        construction_year INTEGER,
        manufacturer VARCHAR,
        model_number VARCHAR,
        manufact_part_number VARCHAR,
        serial_number VARCHAR,
        gross_weight DECIMAL(18, 3),
        unit_of_weight VARCHAR,
        technical_ident_number VARCHAR,
        valid_from DATE,
        display_position VARCHAR,
        catalog_profile VARCHAR,
        company_code INTEGER,
        cost_center INTEGER,
        controlling_area INTEGER,
        maintenance_plant INTEGER,
        main_work_center VARCHAR,
        work_center VARCHAR,
        planning_plant INTEGER,
        plant_section VARCHAR,
        location VARCHAR,
        address_ref INTEGER,
        PRIMARY KEY(equipment_id)
    );
"""

equi_memo_text_ddl = """
    CREATE OR REPLACE TABLE s4_class_rep.equi_long_text(
        equipment_id VARCHAR NOT NULL,
        long_text VARCHAR,
        PRIMARY KEY(equipment_id)
    );
"""

# TODO - generate sql for class tables?

equi_solution_id_ddl = """
    CREATE OR REPLACE TABLE s4_class_rep.equi_solution_id (
        equipment_id VARCHAR NOT NULL,
        idx_solution_id INTEGER,
        solution_id INTEGER,
        PRIMARY KEY(equipment_id)
    );
"""

equi_east_north_ddl = """
    CREATE OR REPLACE TABLE s4_class_rep.equi_east_north (
        equipment_id VARCHAR NOT NULL,
        idx_easting INTEGER,
        easting INTEGER,
        idx_northing INTEGER,
        northing INTEGER,
        PRIMARY KEY(equipment_id)
    );
"""

equi_asset_condition_ddl = """
    CREATE OR REPLACE TABLE s4_class_rep.equi_asset_condition (
        equipment_id VARCHAR NOT NULL,
        idx_condition_grade INTEGER,
        condition_grade VARCHAR,
        idx_condition_grade_reason INTEGER,
        condition_grade_reason VARCHAR,
        idx_survey_date INTEGER,
        survey_date INTEGER,
        PRIMARY KEY(equipment_id)
    );
"""
