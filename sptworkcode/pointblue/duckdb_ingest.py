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

# TODO get rid of hardcoded paths....
duckdb_inserts = """
-- import sqlite data

INSERT INTO telemetry_facts
SELECT 
    tf.os_name AS outstation_name, 
    tf.od_name AS site_sai_num,
    tf.od_comment AS site_name,
    tf.os_comment AS outstation_comment,
    tf.cleansed_os_addr AS outstation_id,
    tf.ai2_pl_ref AS outstation_pli_num
FROM sqlite_scan('g:/work/2023/point_blue/point_blue_imports1.sqlite3', 'telemetry_facts') tf;

INSERT INTO worklist
SELECT 
    w.asset_ref AS asset_id,
    w.date AS submit_timestamp, 
    w.assetname AS asset_name,
    w.status AS status
FROM sqlite_scan('g:/work/2023/point_blue/point_blue_imports1.sqlite3', 'worklist') w;

INSERT INTO s4_equipment_master
SELECT 
    DISTINCT(spb.equipment) AS equi_id,
    spb.description_of_technical_object AS equi_name,
    spb.functional_location AS func_loc,
    spb.superord_equipment AS super_id,
    spb.technical_identification_no AS tag_name,
    spb.user_status AS asset_status,
    spb.object_type AS object_type,
    spb.manufacturer_of_asset AS manufacturer,
    spb.model_number AS model,
    spb.manufacturer_part_number AS specific_model,
    spb.manufactserialnumber AS serial_number,
    spb.start_up_date AS startup_date,
    spb.equipment_category AS equi_category
FROM sqlite_scan('g:/work/2023/point_blue/point_blue_imports1.sqlite3', 's4_point_blue') spb;

INSERT INTO aib_equipment_master
SELECT 
    DISTINCT(apb.reference) AS pli_num,
    apb.common_name AS common_name,
    apb.installed_from AS installed_from,
    apb.manufacturer AS manufacturer,
    apb.model AS model,
    apb.specific_model_frame AS specific_model,
    apb.serial_no AS serial_number,
    apb.assetstatus AS asset_status
FROM sqlite_scan('g:/work/2023/point_blue/point_blue_imports1.sqlite3', 'aib_point_blue') apb;

INSERT INTO aib_equipment_master
SELECT 
    DISTINCT(apb.reference) AS pli_num,
    apb.common_name AS common_name,
    apb.installed_from AS installed_from,
    apb.manufacturer AS manufacturer,
    apb.model AS model,
    apb.specific_model_frame AS specific_model,
    apb.serial_no AS serial_number,
    apb.assetstatus AS asset_status
FROM sqlite_scan('g:/work/2023/point_blue/point_blue_imports1.sqlite3', 'aib_point_blue_4g') apb;
"""