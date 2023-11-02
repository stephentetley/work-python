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

s4_funcloc_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_funcloc_masterdata(
        functional_location TEXT NOT NULL,
        address_ref INTEGER,
        category TEXT,
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
        status TEXT,
        structure_indicator TEXT,
        superior_funct_loc TEXT,
        user_status TEXT,
        work_center TEXT,
        PRIMARY KEY(functional_location)
    );
"""


s4_equipment_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_equipment_masterdata(
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
