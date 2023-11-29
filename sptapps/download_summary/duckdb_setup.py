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


vw_floc_characteristics_summary_ddl = """
    CREATE OR REPLACE VIEW vw_floc_characteristics_summary AS
    SELECT 
        sfm.funcloc_id AS entity_id,
        sfm.description AS description,
        sfm.functional_location AS functional_location,
        sfm.object_type AS object_type,
        sfm.user_status AS user_status,
        wacj.class_type AS class_type,
        wacj.class_name AS class_name, 
        wacj.json_chars AS json_chars,
    FROM s4_fd_funcloc_masterdata sfm
    JOIN vw_worklist_all_classes_json wacj ON wacj.entity_id = sfm.funcloc_id
    WHERE wacj.class_type = '003';
    """

vw_equi_characteristics_summary_ddl = """
    CREATE OR REPLACE VIEW vw_equi_characteristics_summary AS
    SELECT 
        sem.equi_id AS entity_id,
        sem.description AS description,
        sem.functional_location AS functional_location,
        sem.object_type AS object_type,
        sem.user_status AS user_status,
        sem.manufacturer AS manufacturer,
        sem.model_number AS model_number,
        wacj.class_type AS class_type,
        wacj.class_name AS class_name, 
        wacj.json_chars AS json_chars,
    FROM s4_fd_equipment_masterdata sem
    JOIN vw_worklist_all_classes_json wacj ON wacj.entity_id = sem.equi_id
    WHERE wacj.class_type = '002';
    """

