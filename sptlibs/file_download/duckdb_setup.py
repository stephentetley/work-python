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

s4_fd_funcloc_ddl = """
    CREATE OR REPLACE TABLE s4_fd_funcloc(
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


s4_fd_equi_ddl = """
    CREATE OR REPLACE TABLE s4_fd_equi(
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

s4_fd_classes_ddl = """
    -- Same table for classequi and classfloc
    CREATE OR REPLACE TABLE s4_fd_classes(
        entity_id TEXT NOT NULL,
        class_name TEXT NOT NULL,
        class_type TEXT,
    );
    """

s4_fd_char_values_ddl = """
    -- No primary key
    CREATE OR REPLACE TABLE s4_fd_char_values(
        entity_id TEXT NOT NULL,
        char_name TEXT NOT NULL,
        char_desc TEXT,
        char_text_value TEXT,
        class_type TEXT,
        int_counter_value TEXT,
        value_from DECIMAL(26, 6),
        value_to DECIMAL(26,6),
    );
"""

def s4_fd_funcloc_insert(*, sqlite_path: str, funcloc_tablename: str) -> str: 
    return f"""
    INSERT INTO s4_fd_funcloc BY NAME
    SELECT 
        f.funcloc AS functional_location,
        f.bukrsfloc AS company_code,
        IF(f.baumm IS NULL OR f.baumm = '', NULL, CAST(f.baumm AS INTEGER)) AS construction_month,
        IF(f.baujj IS NULL OR f.baujj = '', NULL, CAST(f.baujj AS INTEGER)) AS construction_year,
        f.kokr_floc AS controlling_area,
        f.kost_floc AS cost_center,
        f.txtmi AS description,
        f.usta_floc AS user_status,
        f.fltyp AS category,
        IF(f.iequi = 'X', true, false) AS installation_allowed,
        f.stor_floc AS location,
        f.gewrkfloc AS main_work_center,
        f.swerk_fl AS maintenance_plant,
        f.floc_ref AS masked_functional_location,
        f.eqart AS object_type,
        f.jobjn_fl AS object_number,
        f.plnt_floc AS planning_plant,
        f.beber_fl AS plant_section,
        f.wergwfloc AS plant_for_work_center,
        IF(f.posnr IS NULL OR f.posnr = '', NULL, CAST(f.posnr AS INTEGER)) AS display_position,
        if(f.inbdt IS NOT NULL, strptime(f.inbdt, '%d.%m.%Y'), NULL) AS startup_date,
        f.stattext AS status,
        f.stsm_floc AS status_profile,
        f.ustw_floc AS status_of_an_object,
        f.tplkz_flc AS structure_indicator,
        f.tplma1 AS superior_fl_for_cr_processing,
        f.tplma AS superior_funct_loc,
        f.adrnr AS address_ref,
    FROM sqlite_scan('{sqlite_path}', '{funcloc_tablename}') f;
    """

def s4_fd_equi_insert(*, sqlite_path: str, equi_tablename: str) -> str: 
    return f"""
    INSERT INTO s4_fd_equi BY NAME
    SELECT 
        e.equi AS equi_id,
        e.rbnr_eeqz AS catalog_profile,
        e.bukr_eilo AS company_code,
        e.baumm_eqi AS construction_month,
        e.baujj AS construction_year,
        e.kokr_eilo AS controlling_area,
        e.kost_eilo AS cost_center,
        e.gewrki AS data_origin,
        e.txtmi AS description,
        e.usta_equi AS display_lines_for_user_status,
        e.tpln_eilo AS functional_location,
        e.brgew AS gross_weight,
        e.stor_eilo AS location,
        e.arbp_eeqz AS main_work_center,
        e.swer_eilo AS maintenance_plant,
        e.serge AS serial_number,
        e.mapa_eeqz AS manufact_part_number,
        e.herst AS manufacturer,
        e.typbz AS model_number,
        e.eqart_equ AS object_type,
        e.ppla_eeqz AS planning_plant,
        e.bebe_eilo AS plant_section,
        e.wergw_eqi AS plant_for_work_center,
        e.heqn_eeqz AS display_position,
        IF(e.inbdt IS NOT NULL, strptime(e.data_eeqz, '%d.%m.%Y'), NULL) AS startup_date,
        e.stattext AS status,
        e.stsm_equi AS status_profile,
        e.ustw_equi AS status_of_an_object,
        e.hequ_eeqz AS  superord_id,
        e.tidn_eeqz AS technical_ident_number,
        e.gewei AS unit_of_weight,
        IF(e.data_eeqz IS NOT NULL, strptime(e.data_eeqz, '%d.%m.%Y'), NULL) AS valid_from,
        e.adrnr AS address_ref
    FROM sqlite_scan('{sqlite_path}', '{equi_tablename}') e;
    """

def s4_fd_classfloc_insert(*, sqlite_path: str, class_tablename: str) -> str: 
    return f"""
    INSERT INTO s4_fd_classes BY NAME
    SELECT 
        c.funcloc AS entity_id,
        c.class AS class_name,
        c.classtype AS class_type
    FROM sqlite_scan('{sqlite_path}', '{class_tablename}') c;
    """

def s4_fd_classequi_insert(*, sqlite_path: str, class_tablename: str) -> str: 
    return f"""
    INSERT INTO s4_fd_classes BY NAME
    SELECT 
        c.equi AS entity_id,
        c.class AS class_name,
        c.classtype AS class_type
    FROM sqlite_scan('{sqlite_path}', '{class_tablename}') c;
    """

def s4_fd_char_valuafloc_insert(*, sqlite_path: str, valua_tablename: str) -> str: 
    return f"""
    INSERT INTO s4_fd_char_values BY NAME
    SELECT 
        v.funcloc AS entity_id,
        v.charid AS char_name,
        v.atnam AS char_desc,
        v.atwrt AS char_text_value,
        v.classtype AS class_type,
        v.valcnt AS int_counter_value,
        v.atflv AS value_from,
        v.atflb AS value_to
    FROM sqlite_scan('{sqlite_path}', '{valua_tablename}') v;
    """

def s4_fd_char_valuaequi_insert(*, sqlite_path: str, valua_tablename: str) -> str: 
    return f"""
    INSERT INTO s4_fd_char_values BY NAME
    SELECT 
        v.equi AS entity_id,
        v.charid AS char_name,
        v.atnam AS char_desc,
        v.atwrt AS char_text_value,
        v.classtype AS class_type,
        v.valcnt AS int_counter_value,
        v.atflv AS value_from,
        v.atflb AS value_to
    FROM sqlite_scan('{sqlite_path}', '{valua_tablename}') v;
    """

def s4_classlists_table_copy(*, classlists_duckdb_path: str) -> str: 
    return f"""
        ATTACH '{classlists_duckdb_path}' AS classlists_db;
        CREATE OR REPLACE TABLE s4_classlist_characteristic_defs AS SELECT * FROM classlists_db.s4_characteristic_defs;
        CREATE OR REPLACE TABLE s4_classlist_enum_defs AS SELECT * FROM classlists_db.s4_enum_defs;
        DETACH classlists_db;
    """
