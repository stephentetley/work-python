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

def s4_funcloc_master_insert(*, sqlite_path: str, floc_tablename: str, has_aib_characteritics: bool) -> str: 
    if has_aib_characteritics:
        where_clause = 'WHERE f.s_4_aib_reference IS NOT NULL'
    else:
        where_clause = ''
    return f"""
    INSERT INTO s4_funcloc_masterdata BY NAME
    SELECT 
        f.functional_location AS functional_location,
        f.address_number AS address_ref,
        f.functloccategory AS category,
        f.company_code AS company_code,
        f.construction_month AS construction_month,
        f.construction_year AS construction_year,
        f.controlling_area AS controlling_area,
        f.cost_center AS cost_center,
        f.description_of_functional_location AS description,
        f.position_in_object AS display_position,
        if(f.installation_allowed = 'X', True, False) AS installation_allowed,
        f.location AS location,
        f.maintenance_plant AS maintenance_plant,
        f.main_work_center AS main_work_center,
        f.object_type AS object_type,
        f.object_number AS object_number,
        f.planning_plant AS planning_plant,
        f.plant_section AS plant_section,
        f.start_up_date AS startup_date,
        f.structure_indicator AS structure_indicator,
        f.superior_functional_location AS superior_funct_loc,
        f.system_status AS system_status,
        f.user_status AS user_status,
        f.work_center AS work_center,
    FROM sqlite_scan('{sqlite_path}', '{floc_tablename}') f
    {where_clause};
    """

def s4_equipment_master_insert(*, sqlite_path: str, equi_tablename: str, has_aib_characteritics: bool) -> str: 
    if has_aib_characteritics:
        where_clause = 'WHERE e.s_4_aib_reference IS NOT NULL'
    else:
        where_clause = ''
    return f"""
    INSERT INTO s4_equipment_masterdata BY NAME
    SELECT 
        e.equipment AS equi_id,
        e.address_number AS address_ref,
        e.catalog_profile AS catalog_profile,
        e.equipment_category AS category,
        e.company_code AS company_code,
        e.construction_month AS construction_month,
        e.construction_year AS construction_year,
        e.controlling_area AS controlling_area,
        e.cost_center AS cost_center,
        e.description_of_technical_object AS description,
        e.position AS display_position,
        e.functional_location AS functional_location,
        e.gross_weight AS gross_weight,
        e.location AS location,
        e.main_work_center AS main_work_center,
        e.maintenance_plant AS maintenance_plant,
        e.manufacturer_part_number AS manufact_part_number,
        e.manufacturer_of_asset AS manufacturer,
        e.model_number AS model_number,
        e.object_type AS object_type,
        e.planning_plant AS planning_plant,
        e.plant_section AS plant_section,
        e.manufactserialnumber AS serial_number,
        e.start_up_date AS startup_date,
        e.superord_equipment AS  superord_id,
        e.technical_identification_no AS technical_ident_number,
        e.weight_unit AS unit_of_weight,
        e.user_status AS user_status,
        e.valid_from AS valid_from,
        e.work_center AS work_center,
    FROM sqlite_scan('{sqlite_path}', '{equi_tablename}') e
    {where_clause};
    """