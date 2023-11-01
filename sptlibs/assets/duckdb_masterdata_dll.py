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
        company_code INTEGER,
        construction_month INTEGER,
        construction_year INTEGER,
        controlling_area INTEGER,
        cost_center INTEGER,
        description TEXT,
        user_status TEXT,
        category TEXT,
        installation_allowed BOOLEAN,
        location TEXT,
        main_work_center TEXT,
        maintenance_plant INTEGER, 
        masked_functional_location TEXT,
        object_type TEXT,
        object_number TEXT,
        planning_plant INTEGER,
        plant_section TEXT,
        plant_for_work_center INTEGER,
        display_position INTEGER,
        startup_date DATE,
        status TEXT,
        status_profile TEXT,
        status_of_an_object TEXT,
        structure_indicator TEXT,
        superior_fl_for_cr_processing TEXT,
        superior_funct_loc TEXT,
        address_ref INTEGER,
        PRIMARY KEY(functional_location)
    );
"""


s4_equipment_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_equipment_masterdata(
        equi_id TEXT NOT NULL,
        catalog_profile TEXT,
        company_code INTEGER,
        construction_month INTEGER,
        construction_year INTEGER,
        controlling_area INTEGER,
        cost_center INTEGER,
        data_origin TEXT,
        description TEXT,
        display_lines_for_user_status TEXT,
        functional_location TEXT, 
        gross_weight DECIMAL,
        location TEXT,
        main_work_center TEXT,
        maintenance_plant INTEGER,
        serial_number TEXT,
        manufact_part_number TEXT,
        manufacturer TEXT,
        model_number TEXT,
        object_type TEXT,
        planning_plant INTEGER,
        plant_section TEXT,
        plant_for_work_center INTEGER,
        display_position INTEGER,
        startup_date DATE,
        status TEXT,
        status_profile TEXT,
        status_of_an_object TEXT,
        superord_id TEXT,
        technical_ident_number TEXT,
        unit_of_weight TEXT,
        valid_from DATE,
        address_ref INTEGER,
        PRIMARY KEY(equi_id)
    );
"""
