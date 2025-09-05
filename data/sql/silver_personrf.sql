CREATE SCHEMA IF NOT EXISTS silver;

INSERT INTO silver.silver_personrf
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(PER_NO AS INTEGER) AS per_no,
        TRY_CAST(PERSONRF AS INTEGER) AS personrf,
        PERSONRFNAME AS personrf_name
FROM bronze.bronze_personrf
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM silver.silver_personrf s
            WHERE s.st_case = TRY_CAST(bronze.bronze_personrf.ST_CASE AS INTEGER)
                AND s.veh_no = TRY_CAST(bronze.bronze_personrf.VEH_NO AS INTEGER)
                AND s.per_no = TRY_CAST(bronze.bronze_personrf.PER_NO AS INTEGER)
    );
