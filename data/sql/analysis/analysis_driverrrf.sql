INSERT INTO analysis.analysis_driverrf
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(DRIVERRF AS INTEGER) AS driverrf,
    DRIVERRFNAME AS driverrf_name
FROM bronze.bronze_driverrf
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.analysis_driverrf a
        WHERE a.st_case = TRY_CAST(bronze.bronze_driverrf.ST_CASE AS INTEGER)
            AND a.veh_no = TRY_CAST(bronze.bronze_driverrf.VEH_NO AS INTEGER)
    );