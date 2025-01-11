-- TODO - only precede 1-4 levels for level 5, level 6 only generate level 6

WITH 
cte1 AS (
    SELECT 
        t.requested_floc AS floc, 
        t.name AS source_name,
        t.objtype AS source_type,
        t.classtype AS source_class_type,
        regexp_split_to_array(floc, '-') AS arr,
        len(arr) as floc_category,
        list_extract(arr, 1) as site,
        list_extract(arr, 2) AS func,
        list_extract(arr, 3) AS proc_grp,
        list_extract(arr, 4) AS proc,
        list_extract(arr, 5) AS sysm,
        list_extract(arr, 6) AS subsysm,
        IF (func        IS NOT NULL, concat_ws('-', site, func), NULL) AS level2,
        IF (proc_grp    IS NOT NULL, concat_ws('-', site, func, proc_grp), NULL) AS level3,
        IF (proc        IS NOT NULL, concat_ws('-', site, func, proc_grp, proc), NULL) AS level4,
        -- TODO - generate either level 5 or level 6 depending on floc size
        
        IF (floc_category = 5, concat_ws('-', site, func, proc_grp, proc, sysm), NULL) AS level5,
        IF (floc_category = 6, concat_ws('-', site, func, proc_grp, proc, sysm, subsysm), NULL) AS level6,
    FROM raw_data.worklist t
    ),
    cte2 AS (
        SELECT 
            t1.*,
            t2.description AS name_2,
            t3.description AS name_3,
            t4.description AS name_4,
        FROM cte1 t1
        LEFT JOIN s4_ztables.flocdes t2 ON t2.objtype = t1.func
        LEFT JOIN s4_ztables.flocdes t3 ON t3.objtype = t1.proc_grp
        LEFT JOIN s4_ztables.flocdes t4 ON t4.objtype = t1.proc
    )
(SELECT  
    site AS floc,  
    source_name AS name,
    1 AS floc_category,
    'SITE' AS floc_type,
    NULL AS floc_class,
FROM cte2 WHERE cte2.floc_category = 1)
UNION BY NAME
(SELECT 
    level2 AS floc,
    name_2 AS name,
    2 AS floc_category,
    func AS floc_type,
    NULL AS floc_class,
FROM cte2 WHERE cte2.level2 IS NOT NULL)
UNION BY NAME 
(SELECT 
    level3 AS floc, 
    name_3 AS name,
    3 AS floc_category,
    proc_grp AS floc_type,
    NULL AS floc_class,
FROM cte2 WHERE cte2.level3 IS NOT NULL)
UNION BY NAME
(SELECT 
    level4 AS floc,
    name_4 AS name,
    4 AS floc_category,
    proc AS floc_type,
    NULL AS floc_class,
FROM cte2 WHERE cte2.level4 IS NOT NULL)
UNION BY NAME
(SELECT 
    level5 AS floc, 
    source_name AS name, 
    5 AS floc_category,
    source_class_type AS floc_class,
    source_type AS floc_type, 
FROM cte2 WHERE cte2.floc_category = 5)
UNION BY NAME
(SELECT 
    level6 AS floc, 
    source_name AS name,
    6 AS floc_category,
    source_type AS floc_type, 
    NULL AS floc_class,
FROM cte2 WHERE cte2.floc_category = 6)
ORDER BY floc
;