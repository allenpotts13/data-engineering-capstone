CREATE SCHEMA IF NOT EXISTS analysis;

INSERT INTO analysis.silver_nmprior
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(PER_NO AS INTEGER) AS per_no,
    TRY_CAST(NMACTION AS INTEGER) AS nmaction,
    NMACTIONNAME AS nmaction_name
FROM bronze.bronze_nmprior
WHERE ST_CASE IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.silver_nmprior a
        WHERE a.st_case = TRY_CAST(bronze.bronze_nmprior.ST_CASE AS INTEGER)
            AND a.veh_no = TRY_CAST(bronze.bronze_nmprior.VEH_NO AS INTEGER)
            AND a.per_no = TRY_CAST(bronze.bronze_nmprior.PER_NO AS INTEGER)
    );