INSERT INTO analysis.analysis_vsoe
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(VEVENTNUM AS INTEGER) AS veventnum,
        TRY_CAST(SOE AS INTEGER) AS soe,
        SOENAME AS soe_name,
        TRY_CAST(AOI AS INTEGER) AS aoi,
        AOINAME AS aoi_name
FROM bronze.bronze_vsoe
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL AND VEVENTNUM IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM analysis.analysis_vsoe a
            WHERE a.st_case = TRY_CAST(bronze.bronze_vsoe.ST_CASE AS INTEGER)
                AND a.veh_no = TRY_CAST(bronze.bronze_vsoe.VEH_NO AS INTEGER)
                AND a.veventnum = TRY_CAST(bronze.bronze_vsoe.VEVENTNUM AS INTEGER)
    );
