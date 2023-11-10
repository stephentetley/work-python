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

import sptlibs.export_utils as export_utils


def output_equi_summary_report(*, duckdb_path: str, csv_outpath: str) -> str:
    export_utils.output_csv_report(duckdb_path=duckdb_path, select_stmt=equi_summary_report, csv_outpath=csv_outpath)

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
        sem.serial_number AS serial_number,
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
        gcn.class_name AS class_type,
        gcl.classes AS equi_classes,
    FROM 
        s4_equipment_masterdata sem
    JOIN vw_get_class_name gcn ON gcn.entity_id = sem.equi_id
    JOIN vw_get_classes_list gcl ON gcl.entity_id = sem.equi_id 
    """
