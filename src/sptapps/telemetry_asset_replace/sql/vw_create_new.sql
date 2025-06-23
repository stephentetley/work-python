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

CREATE OR REPLACE MACRO generate_outstation_name(rtu_id, prefix) AS
    concat_ws(' ', rtu_id, prefix, 'Outstation') 
;

CREATE OR REPLACE MACRO norm_os_address(os_addr) AS
    (os_addr).replace(' ', '').replace(',', '_')
;



SELECT 
	t.ai2_item_to_derive_new_equi_from  AS 'pli_num',
	generate_outstation_name(t.rtu_id, t.prefix) AS 'description',
	t1.os_name AS 'tech_ident_no',
	t2.manufacturer AS 'manufacturer',
	t.* EXCLUDE (t.ai2_item_to_derive_new_equi_from)
FROM telemetry_landing.worklist t
JOIN rts_outstations.outstations t1 ON norm_os_address(t1.os_address) = t.rtu_id
JOIN ai2_export.equi_master_data t2 ON t2.ai2_reference = t.ai2_item_to_derive_new_equi_from
;