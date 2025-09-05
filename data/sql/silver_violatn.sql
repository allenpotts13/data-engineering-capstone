CREATE OR REPLACE TABLE silver_violatn AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    LOWER(statename) AS statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(veh_no AS INTEGER) AS veh_no,
    TRY_CAST(violation AS INTEGER) AS violation,
    LOWER(violationname) AS violation_name
FROM bronze_violatn
WHERE st_case IS NOT NULL AND veh_no IS NOT NULL;
