CREATE OR REPLACE TABLE silver_vsoe AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    LOWER(statename) AS statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(veh_no AS INTEGER) AS veh_no,
    TRY_CAST(veventnum AS INTEGER) AS vevent_num,
    TRY_CAST(soe AS INTEGER) AS soe,
    LOWER(soename) AS soe_name,
    TRY_CAST(aoi AS INTEGER) AS aoi,
    LOWER(aoiname) AS aoi_name
FROM bronze_vsoe
WHERE st_case IS NOT NULL AND veh_no IS NOT NULL AND veventnum IS NOT NULL;
