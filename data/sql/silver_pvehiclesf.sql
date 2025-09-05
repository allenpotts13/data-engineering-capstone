CREATE OR REPLACE TABLE silver_pvehiclesf AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(veh_no AS INTEGER) AS veh_no,
    TRY_CAST(pvehiclesf AS INTEGER) AS pvehiclesf,
    pvehiclesfname
FROM bronze_pvehiclesf
WHERE st_case IS NOT NULL AND veh_no IS NOT NULL;
