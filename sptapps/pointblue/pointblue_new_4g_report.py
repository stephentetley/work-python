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


def output_new_4g_report(*, duckdb_path: str, csv_outpath: str) -> str:
    export_utils.output_csv_report(duckdb_path=duckdb_path, select_stmt=new_4g_report_body, csv_outpath=csv_outpath)

new_4g_report_body = """
    SELECT 
        DISTINCT(aw.asset_id) AS 'AI2 Ref',
        '' AS '[instructions_create_equipment]',
        tf.outstation_id || ' Point Blue 4G Outstation' AS 'Description',
        'I' AS 'Category',
        sem.func_loc AS 'Functional Location',
        '0010' AS 'Position', 
        'NETW' AS 'Object Type',
        '' AS '[instructions_click_pencil]',
        '' AS '[instructions_click_data_origin]',
        '[Automatic or today''s date]' AS 'Valid From',
        'OPER' AS 'User Status',
        'OPER' AS 'Status with St. No (status number)',
        strftime(aem.installed_from, '%d.%m.%Y') as 'Start-up date',
        'METASPHERE' AS 'Manufacturer',
        'POINT BLUE' AS 'Model',
        'POINT BLUE 4G' AS 'ManufactPartNo',
        lpad(aem.serial_number, 9, '0') AS 'ManufSerialNumber',
        CAST(extract('year' FROM aem.installed_from) AS TEXT) AS 'Constr Yr.', 
        lpad(CAST(extract('month' FROM aem.installed_from) AS TEXT), 2, '0') AS 'Constr Mth.', 
        tf.outstation_name AS 'Technical IdentNo',
        'Installed on the Point Blue Refresh Scheme - AMP7' AS 'Long Text',
        'AIB_REFERENCE' AS '[Add class ''AIB_REFERENCE'']',
        aw.asset_id AS 'AI2 AIB REFERENCE (1)',
        vsai.aib_ai2_reference AS 'AI2 AIB REFERENCE (2)',
        'ASSET_CONDITION' AS '[Add class ''ASSET_CONDITION'']',
        'NEW' AS 'Performance Grade Reason',
        'NEW' AS 'Loading Factor Reason',   
        'NEW' AS 'Condition Grade Reason',
        '1 - GOOD' AS 'Condition Grade',
        '1 - AVAILABILITY 95%' AS 'Performance Grade',
        CAST(extract('year' FROM aem.installed_from) AS TEXT) AS 'Survey Date',  
        '3 - SATISFACTORY' AS 'Loading Factor',  
        'EAST_NORTH' AS '[Add class ''EAST_NORTH'']',
        veasting.field_value AS 'Easting',
        vnorthing.field_value AS 'Northing',
        'NETWTL' AS '[Add class ''NETWTL'']',
        5 AS 'Manufacturers Asset Life (yr)',
        '' AS '[instruction_fields_for_checking]',
        vpli.s4_equi_id AS 'Old S4 Equi_id',
        sem.equi_name AS 'Old S4 Equi_name'
    FROM aib_worklist as aw 
    LEFT OUTER JOIN aib_equipment_master aem ON aw.asset_id = aem.pli_num 
    LEFT OUTER JOIN telemetry_facts tf ON aw.asset_id = tf.outstation_pli_num 
    LEFT OUTER JOIN vw_pli_to_equi_id vpli ON aw.asset_id = vpli.aib_ai2_reference
    LEFT OUTER JOIN s4_equipment_master sem ON vpli.s4_equi_id = sem.equi_id
    LEFT OUTER JOIN vw_equi_id_to_sai vsai ON sem.equi_id = vsai.s4_equi_id
    LEFT OUTER JOIN values_integer veasting ON sem.equi_id = veasting.item_id AND veasting.field_name = 'EASTING'
    LEFT OUTER JOIN values_integer vnorthing ON sem.equi_id = vnorthing.item_id AND vnorthing.field_name = 'NORTHING'
WHERE vpli.s4_equi_id IS NOT NULL
    """
