CREATE SCHEMA IF NOT EXISTS analysis;

INSERT INTO analysis.silver_nmdistract
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(PER_NO AS INTEGER) AS per_no,
    TRY_CAST(NMDISTRACT AS INTEGER) AS nmdistract,
    NMDISTRACTNAME AS nmdistract_name
FROM bronze.bronze_nmdistract
WHERE ST_CASE IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.silver_nmdistract a
        WHERE a.st_case = TRY_CAST(bronze.bronze_nmdistract.ST_CASE AS INTEGER)
            AND a.veh_no = TRY_CAST(bronze.bronze_nmdistract.VEH_NO AS INTEGER)
            AND a.per_no = TRY_CAST(bronze.bronze_nmdistract.PER_NO AS INTEGER)
    );