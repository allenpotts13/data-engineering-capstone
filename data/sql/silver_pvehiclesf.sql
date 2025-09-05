CREATE SCHEMA IF NOT EXISTS silver;

INSERT INTO silver.silver_pvehiclesf
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(PVEHICLESF AS INTEGER) AS pvehiclesf,
        PVEHICLESFNAME AS pvehiclesf_name
FROM bronze.bronze_pvehiclesf
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM silver.silver_pvehiclesf s
            WHERE s.st_case = TRY_CAST(bronze.bronze_pvehiclesf.ST_CASE AS INTEGER)
                AND s.veh_no = TRY_CAST(bronze.bronze_pvehiclesf.VEH_NO AS INTEGER)
    );
