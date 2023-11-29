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
# TODO
# This module contains definitions e.g. `vw_get_class_name_ddl` that should be common to ih06 / ih08.

s4_fd_funcloc_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_fd_funcloc_masterdata(
        funcloc_id TEXT NOT NULL,
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
        structure_indicator TEXT,
        superior_funct_loc TEXT,
        system_status TEXT,
        user_status TEXT,
        work_center TEXT,
        PRIMARY KEY(functional_location)
    );
"""


s4_fd_equipment_masterdata_ddl = """
    CREATE OR REPLACE TABLE s4_fd_equipment_masterdata(
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

vw_entity_worklist_ddl = """
    CREATE OR REPLACE VIEW vw_entity_worklist AS
    SELECT 
        sem.equi_id AS entity_id,
        sem.object_type AS object_type,
        'equi' AS data_source
    FROM s4_fd_equipment_masterdata sem
    UNION 
    SELECT 
        sfm.funcloc_id AS entity_id,
        sfm.object_type AS object_type,
        'floc' AS data_source
    FROM main.s4_fd_funcloc_masterdata sfm;
    """

vw_fd_decimal_values_ddl = """
    CREATE OR REPLACE VIEW vw_fd_decimal_values AS
    SELECT 
        sfcv.entity_id AS entity_id,
        sfcv.class_type AS class_type,
        ew.object_type AS object_type,
        scd.char_precision AS decimal_precision,
        sfc.class_name AS class_name,
        sfcv.char_name AS char_name,
        sfcv.int_counter_value AS int_counter_value,
        CAST(sfcv.value_from AS DECIMAL(26,6)) AS decimal_value,
    FROM s4_fd_char_values sfcv
    JOIN vw_entity_worklist ew ON ew.entity_id = sfcv.entity_id
    JOIN s4_fd_classes sfc ON sfc.entity_id = sfcv.entity_id
    JOIN s4_classlists.characteristic_defs scd ON scd.char_name = sfcv.char_name AND scd.class_name = sfc.class_name AND scd.class_type = sfcv.class_type
    WHERE scd.char_type = 'NUM'
    AND scd.char_precision > 0;
    """

vw_fd_integer_values_ddl = """
    CREATE OR REPLACE VIEW vw_fd_integer_values AS
    SELECT 
        sfcv.entity_id AS entity_id,
        sfcv.class_type AS class_type,
        ew.object_type AS object_type,
        sfc.class_name AS class_name,
        sfcv.char_name AS char_name,
        sfcv.int_counter_value AS int_counter_value,
        CAST(sfcv.value_from AS INTEGER) AS integer_value,
    FROM s4_fd_char_values sfcv
    JOIN vw_entity_worklist ew ON ew.entity_id = sfcv.entity_id
    JOIN s4_fd_classes sfc ON sfc.entity_id = sfcv.entity_id
    JOIN s4_classlists.characteristic_defs scd ON scd.char_name = sfcv.char_name AND scd.class_name = sfc.class_name AND scd.class_type = sfcv.class_type
    WHERE scd.char_type = 'NUM'
    AND scd.char_precision = 0;
    """

vw_fd_text_values_ddl = """
    CREATE OR REPLACE VIEW vw_fd_text_values AS
    SELECT 
        sfcv.entity_id AS entity_id,
        sfcv.class_type AS class_type,
        ew.object_type AS object_type,
        sfc.class_name AS class_name,
        sfcv.char_name AS char_name,
        sfcv.int_counter_value AS int_counter_value,
        sfcv.char_text_value AS text_value,
    FROM s4_fd_char_values sfcv
    JOIN vw_entity_worklist ew ON ew.entity_id = sfcv.entity_id
    JOIN s4_fd_classes sfc ON sfc.entity_id = sfcv.entity_id
    JOIN s4_classlists.characteristic_defs scd ON scd.char_name = sfcv.char_name AND scd.class_name = sfc.class_name AND scd.class_type = sfcv.class_type
    WHERE scd.char_type = 'CHAR';
    """

vw_get_classes_list_ddl = """
    CREATE OR REPLACE VIEW vw_get_classes_list AS
    SELECT 
        sfc.entity_id AS entity_id,
        json_group_array(to_json(sfc.class_name)) AS classes,
    FROM 
        s4_fd_classes sfc
    GROUP BY sfc.entity_id;
    """

vw_get_class_name_ddl =  """
    CREATE OR REPLACE VIEW vw_get_class_name AS
    SELECT 
        ew.entity_id AS entity_id,
        ew.object_type AS object_type,
        fetv.class_name AS class_name,
    FROM vw_entity_worklist ew
    JOIN vw_fd_text_values fetv ON fetv.entity_id = ew.entity_id
    WHERE fetv.char_name = 'UNICLASS_CODE';
    """

vw_fd_all_values_json_ddl = """
    CREATE OR REPLACE VIEW vw_fd_all_values_json AS
    SELECT 
        dv.entity_id AS entity_id,
        dv.class_type AS class_type,
        dv.class_name AS class_name,
        dv.char_name AS char_name,
        to_json(dv.decimal_value) AS json_value,
    FROM vw_fd_decimal_values dv
    UNION
    SELECT
        iv.entity_id AS entity_id,
        iv.class_type AS class_type,
        iv.class_name AS class_name,
        iv.char_name AS char_name,
        to_json(iv.integer_value) AS json_value,
    FROM vw_fd_integer_values iv
    UNION
    SELECT
        tv.entity_id AS entity_id,
        tv.class_type AS class_type,
        tv.class_name AS class_name,
        tv.char_name AS char_name,
        to_json(IF(tv.text_value IS NULL, '', tv.text_value)) AS json_value,
    FROM vw_fd_text_values tv;
    """

vw_fd_characteristics_json_ddl = """
    CREATE OR REPLACE VIEW vw_fd_characteristics_json AS
    SELECT
        entity_id AS entity_id,
        class_name AS class_name,
        json_group_object(char_name, json_array_values) AS json_characteristics,
    FROM
        (SELECT 
            entity_id AS entity_id,
            class_name AS class_name,
            char_name AS char_name,
            json_group_array(json_value) AS json_array_values,
        FROM vw_fd_all_values_json
        GROUP BY entity_id, class_name, char_name
        ORDER BY entity_id, class_name, char_name)
    GROUP BY entity_id, class_name
    ORDER BY entity_id, class_name
    """

vw_fd_classes_json_ddl = """
    CREATE OR REPLACE VIEW vw_fd_classes_json AS
    SELECT
        entity_id AS entity_id,
        json_group_object(class_name, json_characteristics) AS json_classes,
    FROM
        (SELECT
            entity_id AS entity_id,
            class_name AS class_name,
            json_characteristics AS json_characteristics,
        FROM
            vw_fd_characteristics_json
        GROUP BY entity_id, class_name, json_characteristics
        ORDER BY entity_id, class_name)
    GROUP BY entity_id
    ORDER BY entity_id
    """

vw_worklist_all_characteristics_json_ddl = """
    CREATE OR REPLACE VIEW vw_worklist_all_characteristics_json AS
    SELECT 
        ew.entity_id AS entity_id,
        sfc.class_type AS class_type,
        sfc.class_name AS class_name, 
        scd.char_name AS char_name,
        json_group_array(avj.json_value) AS json_array_values,
    FROM
        vw_entity_worklist ew
    JOIN s4_fd_classes sfc ON sfc.entity_id = ew.entity_id 
    JOIN s4_classlists.characteristic_defs scd ON scd.class_type = sfc.class_type AND scd.class_name = sfc.class_name
    LEFT OUTER JOIN vw_fd_all_values_json avj ON ew.entity_id = avj.entity_id AND sfc.class_type = avj.class_type 
                AND sfc.class_name = avj.class_name AND scd.char_name = avj.char_name
    GROUP BY ew.entity_id, sfc.class_type, sfc.class_name, scd.char_name;
    """

vw_worklist_all_classes_json_ddl = """
CREATE OR REPLACE VIEW vw_worklist_all_classes_json AS
SELECT 
    wacj.entity_id AS entity_id,
    wacj.class_type AS class_type,
    wacj.class_name AS class_name, 
    json_group_object(wacj.char_name, json_array_values) AS json_chars,
FROM
    vw_worklist_all_characteristics_json wacj
GROUP BY wacj.entity_id, wacj.class_type, wacj.class_name;
"""

query_get_raw_tables = """
    SELECT 
        t.table_name AS name,
    FROM duckdb_tables() t
    WHERE t.schema_name = 'fd_raw';
    """


s4_fd_funcloc_masterdata_insert = """
    INSERT INTO s4_fd_funcloc_masterdata BY NAME
    SELECT 
        f.funcloc AS funcloc_id,
        f.floc_ref AS functional_location,
        f.adrnr AS address_ref,
        f.bukrsfloc AS company_code,
        TRY_CAST(f.baumm AS INTEGER) AS construction_month,
        TRY_CAST(f.baujj AS INTEGER) AS construction_year,
        f.kokr_floc AS controlling_area,
        f.kost_floc AS cost_center,
        f.txtmi AS description,
        f.usta_floc AS user_status,
        f.fltyp AS category,
        IF(f.iequi = 'X', true, false) AS installation_allowed,
        f.stor_floc AS location,
        f.gewrkfloc AS main_work_center,
        f.swerk_fl AS maintenance_plant,
        f.eqart AS object_type,
        f.jobjn_fl AS object_number,
        f.plnt_floc AS planning_plant,
        f.beber_fl AS plant_section,
        TRY_CAST(f.posnr AS INTEGER) AS display_position,
        IF(f.inbdt IS NOT NULL, strptime(f.inbdt, '%d.%m.%Y'), NULL) AS startup_date,
        f.tplkz_flc AS structure_indicator,
        f.tplma AS superior_funct_loc,
    FROM fd_raw.funcloc_floc1 f;
    """

s4_fd_equipment_masterdata_insert = """
    INSERT INTO s4_fd_equipment_masterdata BY NAME
    SELECT 
        e.equi AS equi_id,
        e.adrnr AS address_ref,
        e.rbnr_eeqz AS catalog_profile,
        e.eqtyp AS category,
        e.bukr_eilo AS company_code,
        e.baumm_eqi AS construction_month,
        e.baujj AS construction_year,
        e.kokr_eilo AS controlling_area,
        e.kost_eilo AS cost_center,
        e.txtmi AS description,
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
        IF(e.heqn_eeqz IS NULL OR e.heqn_eeqz = '', NULL, CAST(e.heqn_eeqz AS INTEGER)) AS display_position,
        IF(e.inbdt IS NOT NULL, strptime(e.inbdt, '%d.%m.%Y'), NULL) AS startup_date,
        e.hequ_eeqz AS superord_id,
        e.stattext AS system_status,
        e.tidn_eeqz AS technical_ident_number,
        e.gewei AS unit_of_weight,
        e.usta_equi AS user_status,
        IF(e.data_eeqz IS NOT NULL, strptime(e.data_eeqz, '%d.%m.%Y'), NULL) AS valid_from,
        e.arbp_eilo AS work_center,
    FROM fd_raw.equi_equi1 e;
    """

s4_fd_classfloc_insert = """
    INSERT INTO s4_fd_classes BY NAME
    SELECT 
        c.funcloc AS entity_id,
        c.class AS class_name,
        c.classtype AS class_type
    FROM fd_raw.classfloc_classfloc1 c;
    """

s4_fd_classequi_insert = """
    INSERT INTO s4_fd_classes BY NAME
    SELECT 
        c.equi AS entity_id,
        c.class AS class_name,
        c.classtype AS class_type
    FROM fd_raw.classequi_classequi1 c;
    """

s4_fd_char_valuafloc_insert = """
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
    FROM fd_raw.valuafloc_valuafloc1 v;
    """

s4_fd_char_valuaequi_insert = """
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
    FROM fd_raw.valuaequi_valuaequi1 v;
    """

