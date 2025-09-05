CREATE OR REPLACE TABLE silver_race AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(veh_no AS INTEGER) AS veh_no,
    TRY_CAST(per_no AS INTEGER) AS per_no,
    TRY_CAST(race AS INTEGER) AS race,
    racename,
    TRY_CAST(order AS INTEGER) AS order,
    ordername,
    TRY_CAST(multrace AS INTEGER) AS multrace,
    multracename
FROM bronze_race
WHERE st_case IS NOT NULL AND veh_no IS NOT NULL AND per_no IS NOT NULL;
