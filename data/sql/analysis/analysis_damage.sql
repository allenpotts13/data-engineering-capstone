INSERT INTO analysis.analysis_damage
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(DAMAGE AS INTEGER) AS damage,
    DAMAGENAME AS damage_name
FROM bronze.bronze_damage
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.analysis_damage a
        WHERE a.st_case = TRY_CAST(bronze.bronze_damage.ST_CASE AS INTEGER)
            AND a.veh_no = TRY_CAST(bronze.bronze_damage.VEH_NO AS INTEGER)
    );