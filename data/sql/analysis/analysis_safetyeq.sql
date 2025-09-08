CREATE SCHEMA IF NOT EXISTS analysis;

INSERT INTO analysis.silver_safetyeq
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(PER_NO AS INTEGER) AS per_no,
        TRY_CAST(NMHELMET AS INTEGER) AS nmhelmet,
        NMHELMETNAME AS nmhelmet_name,
        TRY_CAST(NMPROPAD AS INTEGER) AS nmpropad,
        NMPROPADNAME AS nmpropad_name,
        TRY_CAST(NMOTHPRO AS INTEGER) AS nmothpro,
        NMOTHPRONAME AS nmothpro_name,
        TRY_CAST(NMREFCLO AS INTEGER) AS nmrefclo,
        NMREFCLONAME AS nmrefclo_name,
        TRY_CAST(NMLIGHT AS INTEGER) AS nmlight,
        NMLIGHTNAME AS nmlight_name,
        TRY_CAST(NMOTHPRE AS INTEGER) AS nmothpre,
        NMOTHPRENAME AS nmothpre_name
FROM bronze.bronze_safetyeq
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM analysis.silver_safetyeq s
            WHERE s.st_case = TRY_CAST(bronze.bronze_safetyeq.ST_CASE AS INTEGER)
                AND s.veh_no = TRY_CAST(bronze.bronze_safetyeq.VEH_NO AS INTEGER)
                AND s.per_no = TRY_CAST(bronze.bronze_safetyeq.PER_NO AS INTEGER)
    );
