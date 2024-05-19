DROP TABLE IF EXISTS flocs;
CREATE TEMP TABLE flocs AS SELECT * FROM (VALUES ('SITE1'), ('SITE1-LST')) flocs(floc);

DROP TABLE IF EXISTS worklist;
CREATE TEMP TABLE worklist AS SELECT * FROM (VALUES ('SITE1-LST-OKZ'), ('SITE1-LST-OKB'), 
    ('SITE1-LST-OKZ-FLT'), ('SITE1-LST-OKZ-FLT-SYS01'), ('SITE1-LST-OKZ-FLT-SYS01-ALM01')) worklist(floc);


WITH floc_perms_cte AS (
    SELECT floc FROM (
        SELECT 
            list_aggregate(list_slice(string_split(wl.floc, '-'), 1, 1), 'string_agg', '-') AS floc, 
        FROM  
            worklist wl
        UNION
        SELECT 
            list_aggregate(list_slice(string_split(wl.floc, '-'), 1, 2), 'string_agg', '-') AS floc, 
        FROM  
            worklist wl
        UNION
        SELECT 
            list_aggregate(list_slice(string_split(wl.floc, '-'), 1, 3), 'string_agg', '-') AS floc, 
        FROM  
            worklist wl
        UNION
        SELECT 
            list_aggregate(list_slice(string_split(wl.floc, '-'), 1, 4), 'string_agg', '-') AS floc, 
        FROM  
            worklist wl
        UNION
        SELECT 
            list_aggregate(list_slice(string_split(wl.floc, '-'), 1, 5), 'string_agg', '-') AS floc, 
        FROM  
            worklist wl
        UNION
        SELECT 
            list_aggregate(list_slice(string_split(wl.floc, '-'), 1, 6), 'string_agg', '-') AS floc, 
        FROM  
            worklist wl
    ) AS all_perms
    ANTI JOIN flocs ON all_perms.floc = flocs.floc)
SELECT DISTINCT
    cte.floc AS funcloc,
    string_split(cte.floc, '-') AS __elements,
    len(__elements) AS floc_level,
    CASE 
        WHEN floc_level = 1 THEN 'SITE' 
        WHEN floc_level < 5 THEN list_extract(__elements, floc_level)
        WHEN floc_level = 5 THEN '<system>'
        WHEN floc_level = 6 THEN '<subsystem>'
    END AS obj_type,
FROM floc_perms_cte cte
ORDER BY cte.floc;
