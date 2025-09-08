CREATE SCHEMA IF NOT EXISTS analysis;

INSERT INTO analysis.silver_vehiclesf
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(VEHICLESF AS INTEGER) AS vehiclesf,
        VEHICLESFNAME AS vehiclesf_name
FROM bronze.bronze_vehiclesf
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM analysis.silver_vehiclesf s
            WHERE s.st_case = TRY_CAST(bronze.bronze_vehiclesf.ST_CASE AS INTEGER)
                AND s.veh_no = TRY_CAST(bronze.bronze_vehiclesf.VEH_NO AS INTEGER)
    );
