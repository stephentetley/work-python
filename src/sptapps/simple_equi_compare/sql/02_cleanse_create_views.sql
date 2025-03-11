
CREATE OR REPLACE VIEW telem_raw_data.vw_s4_netw AS 
SELECT DISTINCT ON (t."Equipment") 
    t."Equipment" AS equipment_id,
    t."Description of technical object" AS equi_description,
    t."Functional Location" AS functional_location,
    t."Functional Location"[1:5] AS s4_site,
    t."Position" AS display_position,
    t."Planning Plant" AS planning_plant,
    t."Technical identification no." AS techn_id_num,
    t."Superord. Equipment" AS superord_equi,
    t."Equipment category" AS category,
    t."Start-up date" AS startup_date,
    t."Manufacturer of Asset" AS manufacturer,
    t."Model number" AS model,
    t."Manufacturer part number" AS specific_model_frame,
    t."Manufacturer's Serial Number" AS serial_number,
    t."User Status" AS user_status,
    user_status[1:4] AS simple_status,
    t."Object Type" AS object_type,
    t."Address number" AS address_id,
    t."Gross Weight" AS gross_weight,
    t."Unit of Weight" AS unit_of_weight,
    t1."AI2 AIB Reference" AS pli_num,
    t2."AI2 AIB Reference" AS sai_num,
FROM telem_raw_data.s4_netw t
LEFT JOIN telem_raw_data.s4_netw t1 ON t1."Equipment" = t."Equipment" AND t1."AI2 AIB Reference" LIKE 'PLI%'
LEFT JOIN telem_raw_data.s4_netw t2 ON t2."Equipment" = t."Equipment" AND (t2."AI2 AIB Reference" LIKE 'AFL%' OR t2."AI2 AIB Reference" LIKE 'SAI%')
ORDER BY functional_location
;

CREATE OR REPLACE VIEW telem_raw_data.vw_ai2_outstations AS 
SELECT
    t."Reference" AS pli_num,        
    t."Common Name" AS common_name,
    CASE 
        WHEN common_name LIKE '%CONTROL SERVICES%' THEN 'site_outstation'
        WHEN common_name LIKE '%SEWER MAINTENANCE%' THEN 'level_monitoring_point'
        WHEN common_name LIKE '%SHA/MONITORING%' THEN 'shaft_monitoring'
        ELSE 'unknown'
    END AS outstation_function,
    CASE 
        WHEN outstation_function = 'site_outstation' THEN regexp_extract(common_name, '(.*)/CONTROL SERVICES', 1)
        WHEN outstation_function = 'level_monitoring_point' THEN null
        WHEN outstation_function = 'shaft_monitoring' THEN regexp_extract(common_name, '(.*)/SHA/MONITORING/', 1)
        ELSE ''
    END AS installation_name,
    CASE 
        WHEN outstation_function = 'site_outstation' THEN regexp_extract(common_name, '/RTS MONITORING/(.*)/EQUIPMENT:', 1)
        WHEN outstation_function = 'level_monitoring_point' THEN regexp_extract(common_name, '/SEWER MAINTENANCE/(.*)/EQUIPMENT:', 1)
        WHEN outstation_function = 'shaft_monitoring' THEN null
        ELSE ''
    END AS equi_name,
    regexp_extract(t."Common Name", '.*EQUIPMENT: (.*)$', 1) AS equipment_type,
    t."Installed From" AS startup_date,
    t."Manufacturer" AS manufacturer,
    t."Model" AS model,
    t."Specific Model/Frame" AS specific_model_frame,
    t."Serial No" AS serial_number,
    t."P AND I Tag No" AS pandi_tag,
    t."AssetStatus" AS user_status,
    t."Loc.Ref." AS grid_ref,
    t."Memo Line 1" AS memo_line_1,
    t."Memo Line 2" AS memo_line_2,
    t."Memo Line 3" AS memo_line_3,
    t."Memo Line 4" AS memo_line_4,
    t."Memo Line 5" AS memo_line_5,
FROM telem_raw_data.ai2_outstations t
;

CREATE OR REPLACE VIEW telem_raw_data.vw_site_mapping AS
SELECT
    t."SITE_NAME" as site_name,
    t."AI2_InstallationCommonName" AS installation_name,
    t."AI2_SiteReference" AS site_sai_num,
    t."AI2_InstallationReference" AS inst_sai_num,
    t."AI2_InstallationAssetType" AS installation_type,
    t."Asset Status 02/01/2025" as user_status,
    t."S/4 Hana Floc Lvl1_Code" AS s4_site_code,
    t."S/4 Hana Floc Description" AS s4_site_name,
    t."City Post Code (PostCode) CHAR 10" AS postcode,
FROM telem_raw_data.site_mapping t
;