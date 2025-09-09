INSERT INTO analysis.analysis_cevent
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(EVENTNUM AS INTEGER) AS event_num,
    TRY_CAST(VNUMBER1 AS INTEGER) AS vnumber1,
    TRY_CAST(AOI1 AS INTEGER) AS aoi1,
    AOI1NAME AS aoi1_name,
    TRY_CAST(SOE AS INTEGER) AS soe,
    SOENAME AS soe_name,
    TRY_CAST(VNUMBER2 AS INTEGER) AS vnumber2,
    VNUMBER2NAME AS vnumber2_name,
    TRY_CAST(AOI2 AS INTEGER) AS aoi2,
    AOI2NAME AS aoi2_name
FROM bronze.bronze_cevent
WHERE ST_CASE IS NOT NULL AND EVENTNUM IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM analysis.analysis_cevent a
        WHERE a.st_case = TRY_CAST(bronze.bronze_cevent.ST_CASE AS INTEGER)
            AND a.event_num = TRY_CAST(bronze.bronze_cevent.EVENTNUM AS INTEGER)
    );