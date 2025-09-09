INSERT INTO analysis.analysis_crashrf
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(CRASHRF AS INTEGER) AS crashrf,
    CRASHRFNAME AS crashrf_name
FROM bronze.bronze_crashrf
WHERE ST_CASE IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.analysis_crashrf a
        WHERE a.st_case = TRY_CAST(bronze.bronze_crashrf.ST_CASE AS INTEGER)
            AND a.crashrf = TRY_CAST(bronze.bronze_crashrf.CRASHRF AS INTEGER)
    );