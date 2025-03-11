
--INSTALL excel;
--LOAD excel;

CREATE SCHEMA IF NOT EXISTS telem_raw_data;

CREATE OR REPLACE TABLE telem_raw_data.rts_outstations AS 
SELECT * 
FROM read_csv('G:/work/2025/great_telemetry_reconcile/outstations_tidy.csv')
;


CREATE OR REPLACE TABLE telem_raw_data.site_mapping AS 
SELECT *
FROM read_xlsx('G:/work/2025/great_telemetry_reconcile/SiteMapping.xlsx', header = true);

CREATE OR REPLACE TABLE telem_raw_data.s4_netw AS 
SELECT *
FROM read_xlsx('G:/work/2025/great_telemetry_reconcile/ih08_netwtl_export1.with_aib_reference.xlsx', header = true);

CREATE OR REPLACE TABLE telem_raw_data.ai2_outstations AS 
SELECT *
FROM read_xlsx('G:/work/2025/great_telemetry_reconcile/ai2_export_outstation2.xlsx', header = true, empty_as_varchar= true);


CREATE OR REPLACE TABLE telem_raw_data.ai2_modems AS 
SELECT *
FROM read_xlsx('G:/work/2025/great_telemetry_reconcile/ai2_export_modem.xlsx', header = true);



