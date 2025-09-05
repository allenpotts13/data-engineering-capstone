CREATE OR REPLACE TABLE silver_vehiclesf AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    LOWER(statename) AS statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(veh_no AS INTEGER) AS veh_no,
    TRY_CAST(vehiclesf AS INTEGER) AS vehiclesf,
    LOWER(vehiclesfname) AS vehiclesf_name
FROM bronze_vehiclesf
WHERE st_case IS NOT NULL AND veh_no IS NOT NULL;
