CREATE SCHEMA IF NOT EXISTS analysis;

INSERT INTO analysis.silver_nmimpair
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(PER_NO AS INTEGER) AS per_no,
    TRY_CAST(NMIMPAIR AS INTEGER) AS nmimpair,
    NMIMPAIRNAME AS nmimpair_name
FROM bronze.bronze_nmimpair
WHERE ST_CASE IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.silver_nmimpair a
        WHERE a.st_case = TRY_CAST(bronze.bronze_nmimpair.ST_CASE AS INTEGER)
            AND a.veh_no = TRY_CAST(bronze.bronze_nmimpair.VEH_NO AS INTEGER)
            AND a.per_no = TRY_CAST(bronze.bronze_nmimpair.PER_NO AS INTEGER)
    );