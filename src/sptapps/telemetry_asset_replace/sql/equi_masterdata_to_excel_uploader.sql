-- 
-- Copyright 2025 Stephen Tetley
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
-- http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- 


CREATE OR REPLACE MACRO s4_classrep_to_excel_uploader.translate_equi_masterdata() AS TABLE
SELECT 
    t.equipment_id AS 'equi',
    t.category AS 'category',
    t.equi_description AS 'equi_description',
    t.object_type AS 'object_type',
    t.startup_date AS 'start_up_date',
    t.manufacturer AS 'manufacturer',
    t.model_number AS 'model_number',
    t.manufact_part_number AS 'manuf_part_no',
    t.serial_number AS 'manuf_serial_number',
    t.functional_location AS 'functional_loc',
    t.display_position AS 'position',
    t.technical_ident_number AS 'tech_ident_no',
    'ZEQUIPST' AS 'status_profile',
    t.status_of_an_object AS 'user_status',
FROM s4_classrep.equi_masterdata t
;

INSERT INTO excel_uploader_equi_create.equipment_data BY NAME
SELECT * FROM s4_classrep_to_excel_uploader.translate_equi_masterdata()
ORDER BY equi;
