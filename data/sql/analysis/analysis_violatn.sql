INSERT INTO analysis.analysis_violatn
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(VIOLATION AS INTEGER) AS violation,
        VIOLATIONNAME AS violation_name
FROM bronze.bronze_violatn
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM analysis.analysis_violatn a
            WHERE a.st_case = TRY_CAST(bronze.bronze_violatn.ST_CASE AS INTEGER)
                AND a.veh_no = TRY_CAST(bronze.bronze_violatn.VEH_NO AS INTEGER)
    );
