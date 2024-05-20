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
    con.execute('CREATE SCHEMA IF NOT EXISTS s4_summary;')
    con.execute(s4_summary_funcloc_masterdata_ddl)
    con.execute(s4_summary_equipment_masterdata_ddl)
    con.execute(s4_summary_class_values_ddl)
    con.execute(s4_summary_char_values_ddl)


s4_summary_funcloc_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_summary.funcloc_masterdata(
        floc_id TEXT NOT NULL,
        functional_location TEXT NOT NULL,
        address_ref INTEGER,
        category TEXT,
        catalog_profile TEXT,
        company_code INTEGER,
        construction_month INTEGER,
        construction_year INTEGER,
        controlling_area INTEGER,
        cost_center INTEGER,
        description TEXT,
        display_position INTEGER,
        installation_allowed BOOLEAN,
        location TEXT,
        maintenance_plant INTEGER, 
        main_work_center TEXT,
        object_type TEXT,
        object_number TEXT,
        planning_plant INTEGER,
        plant_section TEXT,
        startup_date DATE,
        structure_indicator TEXT,
        superior_funct_loc TEXT,
        system_status TEXT,
        user_status TEXT,
        work_center TEXT,
        PRIMARY KEY(floc_id)
    );
"""


s4_summary_equipment_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_summary.equipment_masterdata(
        equi_id TEXT NOT NULL,
        address_ref INTEGER,
        catalog_profile TEXT,
        category TEXT,
        company_code INTEGER,
        construction_month INTEGER,
        construction_year INTEGER,
        controlling_area INTEGER,
        cost_center INTEGER,
        description TEXT,
        display_position INTEGER,
        functional_location TEXT, 
        gross_weight DECIMAL,
        location TEXT,
        main_work_center TEXT,
        maintenance_plant INTEGER,
        manufact_part_number TEXT,
        manufacturer TEXT,
        model_number TEXT,
        object_type TEXT,
        planning_plant INTEGER,
        plant_section TEXT,
        serial_number TEXT,
        startup_date DATE,
        superord_id TEXT,
        system_status TEXT,
        technical_ident_number TEXT,
        unit_of_weight TEXT,
        user_status TEXT,
        valid_from DATE,
        work_center TEXT,
        PRIMARY KEY(equi_id)
    );
"""

s4_summary_class_values_ddl = """
    -- Same table for classequi and classfloc
    CREATE OR REPLACE TABLE s4_summary.class_values(
        entity_id TEXT NOT NULL,
        class_name TEXT NOT NULL,
        class_type TEXT,
    );
    """

s4_summary_char_values_ddl = """
    -- No primary key
    CREATE OR REPLACE TABLE s4_summary.char_values(
        entity_id TEXT NOT NULL,
        class_type TEXT,
        class_name TEXT NOT NULL,
        char_name TEXT NOT NULL,
        char_text_value TEXT,
        char_integer_value INTEGER,
        char_decimal_value DECIMAL(26, 6),
    );
"""
