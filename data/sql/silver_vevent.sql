CREATE OR REPLACE TABLE silver_vevent AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    LOWER(statename) AS statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(eventnum AS INTEGER) AS eventnum,
    TRY_CAST(veh_no AS INTEGER) AS veh_no,
    TRY_CAST(veventnum AS INTEGER) AS veventnum,
    TRY_CAST(vnumber1 AS INTEGER) AS vnumber1,
    TRY_CAST(aoi1 AS INTEGER) AS aoi1,
    LOWER(aoi1name) AS aoi1_name,
    TRY_CAST(soe AS INTEGER) AS soe,
    LOWER(soename) AS soename,
    TRY_CAST(vnumber2 AS INTEGER) AS vnumber2,
    LOWER(vnumber2name) AS vnumber2_name,
    TRY_CAST(aoi2 AS INTEGER) AS aoi2,
    LOWER(aoi2name) AS aoi2_name
FROM bronze_vevent
WHERE st_case IS NOT NULL AND veh_no IS NOT NULL AND eventnum IS NOT NULL;
