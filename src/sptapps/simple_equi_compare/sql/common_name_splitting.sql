
--- depends on ai2_metadata.process_group_names and ai2_metadata.process_names


 -- Performant
WITH cte1 AS (
    SELECT 
        t.common_name,
        '/' || t1.process_group_name || '/' AS process_group,
        '/' || t2.process_name || '/' AS process,
    FROM telem_raw_data.vw_ai2_outstations t
    LEFT JOIN ai2_metadata.process_group_names t1 ON contains(t.common_name, '/' || t1.process_group_name || '/')
    JOIN ai2_metadata.process_names t2 ON contains(t.common_name, '/' || t2.process_name || '/')
), cte2 AS (
SELECT 
    t.common_name AS common_name,
    t.process_group AS process_group,
    t.process AS process,
    position(process_group IN common_name) AS processs_group_start,
    position(process IN common_name) AS processs_start,
    position('/EQUIPMENT:' IN common_name) AS equipment_start,
    CASE 
        WHEN processs_group_start > 0 THEN processs_group_start-1
        WHEN processs_start > 0 THEN processs_start-1
        ELSE 0
    END AS site_name_end,
    common_name[1:site_name_end] AS site,
    CASE 
        WHEN processs_start > 0 THEN processs_start + length(process)
        ELSE 0
    END AS item_name_start,
    CASE
        WHEN processs_start > 0 AND equipment_start > 0 THEN common_name[item_name_start:equipment_start-1]
        WHEN processs_start > 0 THEN common_name[item_name_start:] 
        ELSE ''
    END AS item_name,
    CASE
        WHEN equipment_start > 0 THEN common_name[equipment_start+12:]
        ELSE '' 
    END AS equipment_type,
    FROM cte1 t
)
SELECT common_name, min(site) AS site, item_name, equipment_type
FROM cte2
WHERE site <> '' AND item_name <> ''
GROUP BY common_name, item_name, equipment_type
;
