INSERT INTO analysis.analysis_race
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(PER_NO AS INTEGER) AS per_no,
        TRY_CAST(RACE AS INTEGER) AS race,
        RACENAME AS race_name,
        TRY_CAST("ORDER" AS INTEGER) AS order,
        ORDERNAME AS order_name,
        TRY_CAST(MULTRACE AS INTEGER) AS multrace,
        MULTRACENAME AS multrace_name
FROM bronze.bronze_race
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM analysis.analysis_race a
            WHERE a.st_case = TRY_CAST(bronze.bronze_race.ST_CASE AS INTEGER)
                AND a.veh_no = TRY_CAST(bronze.bronze_race.VEH_NO AS INTEGER)
                AND a.per_no = TRY_CAST(bronze.bronze_race.PER_NO AS INTEGER)
                AND a.race = TRY_CAST(bronze.bronze_race.RACE AS INTEGER)
    );
