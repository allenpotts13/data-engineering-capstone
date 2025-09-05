CREATE OR REPLACE TABLE silver_safetyeq AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(veh_no AS INTEGER) AS veh_no,
    TRY_CAST(per_no AS INTEGER) AS per_no,
    TRY_CAST(nmhelmet AS INTEGER) AS nmhelmet,
    nmhelmetname,
    TRY_CAST(nmpropad AS INTEGER) AS nmpropad,
    nmpropadname,
    TRY_CAST(nmothpro AS INTEGER) AS nmothpro,
    nmothproname,
    TRY_CAST(nmrefclo AS INTEGER) AS nmrefclo,
    nmrefcloname,
    TRY_CAST(nmlight AS INTEGER) AS nmlight,
    nmlightname,
    TRY_CAST(nmothpre AS INTEGER) AS nmothpre,
    nmothprename
FROM bronze_safetyeq
WHERE st_case IS NOT NULL AND veh_no IS NOT NULL AND per_no IS NOT NULL;
