CREATE OR REPLACE TABLE silver_vision AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    LOWER(statename) AS statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(veh_no AS INTEGER) AS veh_no,
    TRY_CAST(vision AS INTEGER) AS vision,
    LOWER(visionname) AS vision_name
FROM bronze_vision
WHERE st_case IS NOT NULL AND veh_no IS NOT NULL;
