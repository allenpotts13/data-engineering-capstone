INSERT INTO analysis.analysis_miper
SELECT
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(PER_NO AS INTEGER) AS per_no,
    TRY_CAST(P1 AS INTEGER) AS p1,
    TRY_CAST(P2 AS INTEGER) AS p2,
    TRY_CAST(P3 AS INTEGER) AS p3,
    TRY_CAST(P4 AS INTEGER) AS p4,
    TRY_CAST(P5 AS INTEGER) AS p5,
    TRY_CAST(P6 AS INTEGER) AS p6,
    TRY_CAST(P7 AS INTEGER) AS p7,
    TRY_CAST(P8 AS INTEGER) AS p8,
    TRY_CAST(P9 AS INTEGER) AS p9,
    TRY_CAST(P10 AS INTEGER) AS p10
FROM bronze.bronze_miper
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.analysis_miper a
        WHERE a.st_case = TRY_CAST(bronze.bronze_miper.ST_CASE AS INTEGER)
            AND a.veh_no = TRY_CAST(bronze.bronze_miper.VEH_NO AS INTEGER)
            AND a.per_no = TRY_CAST(bronze.bronze_miper.PER_NO AS INTEGER)
    );