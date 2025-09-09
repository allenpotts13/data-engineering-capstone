INSERT INTO analysis.analysis_vision
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(VISION AS INTEGER) AS vision,
        VISIONNAME AS vision_name
FROM bronze.bronze_vision
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM analysis.analysis_vision a
            WHERE a.st_case = TRY_CAST(bronze.bronze_vision.ST_CASE AS INTEGER)
                AND a.veh_no = TRY_CAST(bronze.bronze_vision.VEH_NO AS INTEGER)
    );
