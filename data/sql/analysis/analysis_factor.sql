CREATE SCHEMA IF NOT EXISTS analysis;

INSERT INTO analysis.silver_factor
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(VEHICLECC AS INTEGER) AS vehiclecc,
    VEHICLECCNAME AS vehiclecc_name
FROM bronze.bronze_factor
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.silver_factor a
        WHERE a.st_case = TRY_CAST(bronze.bronze_factor.ST_CASE AS INTEGER)
            AND a.veh_no = TRY_CAST(bronze.bronze_factor.VEH_NO AS INTEGER)
    );