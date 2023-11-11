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


floc_summary_report = """
    SELECT 
        sfm.funcloc_id AS funcloc_id,
        sfm.functional_location AS functional_location,
        sfm.description AS description,
        sfm.superior_funct_loc AS superior_functional_location,
        sfm.structure_indicator AS structure_indicator,
        sfm.category AS category,
        sfm.object_type AS object_type,
        strftime(sfm.startup_date, '%d.%m.%Y') AS startup_date,
        sfm.installation_allowed AS installation_allowed,
        sfm.system_status AS system_status,
        sfm.user_status AS user_status,
        sfm.planning_plant AS planning_plant,
        sfm.plant_section AS plant_section, 
        sfm.location AS location, 
        sfm.main_work_center AS main_work_center,
        sfm.maintenance_plant AS maintenance_plant,
        sfm.company_code AS company_code,
        sfm.controlling_area AS controlling_area,
        sfm.cost_center AS cost_center,
        sfm.work_center AS work_center,
        lpad(CAST(sfm.display_position AS TEXT), 4, '0') AS display_position,
        sfm.address_ref AS address_ref,
        gcn.class_name AS class_name,
        gcl.classes AS floc_classes,
    FROM 
        s4_funcloc_masterdata sfm
    LEFT OUTER JOIN vw_get_class_name gcn ON gcn.entity_id = sfm.funcloc_id
    JOIN vw_get_classes_list gcl ON gcl.entity_id = sfm.funcloc_id
    ORDER BY sfm.functional_location;
    """

equi_summary_report = """
    SELECT 
        sem.equi_id AS equi_id,
        sem.description AS description,
        sem.functional_location AS functional_location,
        sem.superord_id AS superord_id,
        sem.category AS category,
        sem.object_type AS object_type,
        strftime(sem.startup_date, '%d.%m.%Y') AS startup_date,
        sem.construction_year AS construction_year,
        lpad(CAST(sem.construction_month AS TEXT), 2, '0') AS construction_month,
        sem.gross_weight AS gross_weight,
        sem.unit_of_weight AS unit_of_weight,
        strftime(sem.valid_from, '%d.%m.%Y') AS valid_from,
        sem.system_status AS system_status,
        sem.user_status AS user_status,
        sem.manufacturer AS manufacturer,
        sem.model_number AS model_number,
        sem.manufact_part_number AS manufact_part_number,
        sem.serial_number AS manufact_serial_number,
        sem.planning_plant AS planning_plant,
        sem.plant_section AS plant_section, 
        sem.location AS location, 
        sem.main_work_center AS main_work_center,
        sem.catalog_profile AS catalog_profile,
        sem.maintenance_plant AS maintenance_plant,
        sem.company_code AS company_code,
        sem.controlling_area AS controlling_area,
        sem.cost_center AS cost_center,
        sem.work_center AS work_center,
        lpad(CAST(sem.display_position AS TEXT), 4, '0') AS display_position,
        sem.technical_ident_number AS technical_ident_number,
        sem.address_ref AS address_ref,
        gcn.class_name AS class_name,
        gcl.classes AS equi_classes,
    FROM 
        s4_equipment_masterdata sem
    JOIN vw_get_class_name gcn ON gcn.entity_id = sem.equi_id
    JOIN vw_get_classes_list gcl ON gcl.entity_id = sem.equi_id 
    ORDER BY sem.equi_id;
    """

get_equi_classes_used_query = """
    SELECT DISTINCT 
        cs.class_type,
        cs.class_name
    FROM vw_equi_characteristics_summary cs;
    """

get_floc_classes_used_query = """
    SELECT DISTINCT 
        cs.class_type,
        cs.class_name
    FROM vw_floc_characteristics_summary cs;
    """

equi_class_tab_summary_report = """
    SELECT 
        ecs.entity_id AS entity_id,
        ecs.description AS description,
        ecs.functional_location AS functional_location,
        ecs.object_type AS object_type,
        ecs.user_status AS user_status,
        ecs.manufacturer AS manufacturer,
        ecs.model_number AS model_number,
        ecs.json_chars AS json_chars,
    FROM vw_equi_characteristics_summary ecs
    WHERE 
        ecs.class_type = '002'
    AND ecs.class_name = $class_name
    ORDER BY ecs.entity_id;
    """

floc_class_tab_summary_report = """
    SELECT 
        cs.entity_id AS entity_id,
        cs.description AS description,
        cs.functional_location AS functional_location,
        cs.object_type AS object_type,
        cs.user_status AS user_status,
        cs.json_chars AS json_chars,
    FROM vw_floc_characteristics_summary cs
    WHERE 
        cs.class_type = '003'
    AND cs.class_name = $class_name
    ORDER BY cs.entity_id;
    """
