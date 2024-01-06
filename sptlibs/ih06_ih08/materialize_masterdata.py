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

import duckdb

def materialize_masterdata(*, con: duckdb.DuckDBPyConnection) -> None:
    con.execute(s4_summary_funcloc_masterdata_insert)
    con.execute(s4_summary_equipment_masterdata_insert)


s4_summary_funcloc_masterdata_insert = """
    --- source table has _duplicates_ which cause an error without the row_number interior table 
    INSERT OR REPLACE INTO s4_summary.funcloc_masterdata BY NAME
    SELECT 
        f.standard_indicator_internal AS floc_id,
        f.functional_location AS functional_location,
        f.address_number AS address_ref,
        f.catalog_profile AS catalog_profile,
        f.functloccategory::TEXT AS category,
        f.company_code AS company_code,
        f.construction_month AS construction_month,
        f.construction_year AS construction_year,
        f.controlling_area AS controlling_area,
        f.cost_center AS cost_center,
        f.description_of_functional_location AS description,
        f.position_in_object AS display_position,
        IF(f.installation_allowed = 'X', true, false) AS installation_allowed,
        f.location AS location,
        f.maintenance_plant AS maintenance_plant, 
        f.main_work_center AS main_work_center,
        f.object_type AS object_type,
        f.object_number AS object_number,
        f.planning_plant AS planning_plant,
        f.plant_section AS plant_section,
        f.start_up_date::TIMESTAMP::DATE AS startup_date,
        f.structure_indicator AS structure_indicator,
        f.superior_functional_location AS superior_funct_loc,
        f.system_status AS system_status,
        f.user_status AS user_status,
        f.work_center AS work_center,
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY f1.functional_location) AS rownum,
        FROM s4_ihx_raw_data.floc_masterdata f1
        ) f
    WHERE f.rownum = 1
"""

s4_summary_equipment_masterdata_insert = """
    --- source table has _duplicates_ which cause an error without the row_number interior table 
    INSERT OR REPLACE INTO s4_summary.equipment_masterdata BY NAME
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
        e.functional_location AS functional_location,
        e.gross_weight AS gross_weight,
        e.location AS location,
        e.main_work_center AS main_work_center,
        e.maintenance_plant AS maintenance_plant,
        e.manufactserialnumber AS serial_number,
        e.manufacturer_part_number AS manufact_part_number,
        e.manufacturer_of_asset AS manufacturer,
        e.model_number AS model_number,
        e.object_type AS object_type,
        e.planning_plant AS planning_plant,
        e.plant_section AS plant_section,
        IF(e.position IS NULL, NULL, CAST(e.position AS INTEGER)) AS display_position,
        e.start_up_date::TIMESTAMP::DATE AS startup_date,
        IF(e.superord_equipment IS NOT NULL, e.superord_equipment::INT::TEXT, NULL) AS superord_id,
        e.system_status AS system_status,
        e.technical_identification_no AS technical_ident_number,
        e.weight_unit AS unit_of_weight,
        e.user_status AS user_status,
        e.valid_from::TIMESTAMP::DATE AS valid_from,
        e.work_center AS work_center,
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY e1.equipment) AS rownum,
        FROM s4_ihx_raw_data.equi_masterdata e1
        ) e
    WHERE e.rownum = 1
"""
