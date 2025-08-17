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


-- asset replace / create new
CREATE OR REPLACE VIEW asset_replace.vw_dollar_numbers AS
SELECT 
	printf('$%04d', 100 + row_number() OVER ()) AS fresh_number,
	t.ai2_item_to_derive_new_equi_from AS ai2_reference
FROM telemetry_landing.worklist t
;


CREATE OR REPLACE MACRO simple_outstation_model(ai2_model) AS
CASE 
	WHEN ai2_model = 'MMIM' THEN 'MMIM'
	WHEN ai2_model LIKE 'POINT BLUE%' THEN 'Point Blue'
	WHEN ai2_model LIKE 'POINT GREEN%' THEN 'Point Green'
	WHEN ai2_model LIKE 'POINT ORANGE%' THEN 'Point Orange'
	ELSE ai2_model 
END
;	

CREATE OR REPLACE MACRO manuf_part_number(ai2_model) AS
CASE 
	WHEN ai2_model = 'MMIM' THEN 'MK5 MMIM'
	WHEN ai2_model LIKE 'POINT %' THEN ai2_model
	ELSE 'TO BE DETERMINED'
END
;	

CREATE OR REPLACE MACRO metasphere_outstation_model(ai2_model) AS
CASE 
	WHEN ai2_model = 'MMIM' THEN 'MMIM'
	WHEN ai2_model LIKE 'POINT BLUE%' THEN 'POINT BLUE'
	WHEN ai2_model LIKE 'POINT GREEN%' THEN 'POINT GREEN 3G RTU'
	WHEN ai2_model LIKE 'POINT ORANGE %G' THEN ai2_model
	WHEN ai2_model LIKE 'POINT ORANGE%' THEN 'POINT ORANGE'
	ELSE 'TO BE DETERMINED'
END
;	

CREATE OR REPLACE MACRO cleanse_serial_number(serial_number) AS
CASE 
	WHEN serial_number IS NULL THEN 'TO BE DETERMINED'
	WHEN serial_number = '' THEN 'TO BE DETERMINED'
	WHEN regexp_full_match(serial_number, '[\d]{7}') THEN format('00{:s}', serial_number)
	WHEN regexp_full_match(serial_number, '[\d]{8}') THEN format('0{:s}', serial_number)
	ELSE trim(serial_number)
END
;	

CREATE OR REPLACE MACRO norm_os_address(os_addr) AS
    (os_addr).regexp_replace('\s+', '').replace(',', '_')
;

CREATE OR REPLACE MACRO generate_outstation_name(rtu_id, prefix, ai2_model) AS
    concat_ws(' ', rtu_id, prefix, simple_outstation_model(ai2_model), 'Outstation') 
;


CREATE OR REPLACE VIEW asset_replace.vw_s4_destination_floc AS 
SELECT 
    t.ai2_item_to_derive_new_equi_from AS ai2_reference,
    t1.functional_location AS replaced_equi_floc,
    t.s4_floc AS written_floc,
    if(replaced_equi_floc IS NULL, written_floc, replaced_equi_floc) AS destination_floc,
FROM telemetry_landing.worklist t
LEFT JOIN ih08_landing.export1 t1 ON t1.equipment = t.s4_equipment_to_delete 
;

DELETE FROM s4_classrep.equi_masterdata;


INSERT INTO s4_classrep.equi_masterdata BY NAME
SELECT 
	t1.fresh_number AS 'equipment_id',
	generate_outstation_name(t.rtu_id, t.prefix, t3.model) AS 'equi_description',
	t4.destination_floc AS functional_location,
	'I' AS 'category',
	'NETW' AS 'object_type',
	udfx.get_s4_asset_status(t3.asset_status) AS 'status_of_an_object',
	t3.installed_from AS startup_date,
	t3.manufacturer AS 'manufacturer',
	metasphere_outstation_model(t3.model) AS 'model_number',
	manuf_part_number(t3.model) AS 'manufact_part_number',
	cleanse_serial_number(v2.attribute_value) AS 'serial_number',	
	t2.os_name AS 'technical_ident_number',
	10 AS display_position,
	'NETWTL' AS catalog_profile,
FROM telemetry_landing.worklist t
JOIN asset_replace.vw_dollar_numbers t1 ON t1.ai2_reference = t.ai2_item_to_derive_new_equi_from
JOIN rts_outstations.outstations t2 ON norm_os_address(t2.os_address) = t.rtu_id
JOIN ai2_export.equi_masterdata t3 ON t3.ai2_reference = t.ai2_item_to_derive_new_equi_from
JOIN asset_replace.vw_s4_destination_floc t4 ON t4.ai2_reference = t.ai2_item_to_derive_new_equi_from 
LEFT JOIN ai2_export.equi_eavdata v1 ON v1.ai2_reference = t.ai2_item_to_derive_new_equi_from AND v1.attribute_name = 'specific_model_frame'
LEFT JOIN ai2_export.equi_eavdata v2 ON v2.ai2_reference = t.ai2_item_to_derive_new_equi_from AND v2.attribute_name = 'serial_no'
;

