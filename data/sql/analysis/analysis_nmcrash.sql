INSERT INTO analysis.analysis_nmcrash
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(PER_NO AS INTEGER) AS per_no,
    TRY_CAST(NMCC AS INTEGER) AS nmcc,
    NMCCNAME AS nmcc_name
FROM bronze.bronze_nmcrash
WHERE ST_CASE IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.analysis_nmcrash a
        WHERE a.st_case = TRY_CAST(bronze.bronze_nmcrash.ST_CASE AS INTEGER)
            AND a.veh_no = TRY_CAST(bronze.bronze_nmcrash.VEH_NO AS INTEGER)
            AND a.per_no = TRY_CAST(bronze.bronze_nmcrash.PER_NO AS INTEGER)
    );