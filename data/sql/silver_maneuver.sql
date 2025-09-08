CREATE SCHEMA IF NOT EXISTS silver;

INSERT INTO silver.silver_maneuver
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(MANEUVER AS INTEGER) AS maneuver,
    MANEUVERNAME AS maneuver_name
FROM bronze.bronze_maneuver
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM silver.silver_maneuver s
        WHERE s.st_case = TRY_CAST(bronze.bronze_maneuver.ST_CASE AS INTEGER)
            AND s.veh_no = TRY_CAST(bronze.bronze_maneuver.VEH_NO AS INTEGER)
    );