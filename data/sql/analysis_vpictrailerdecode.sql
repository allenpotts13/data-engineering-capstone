CREATE SCHEMA IF NOT EXISTS analysis;

INSERT INTO analysis.silver_vpictrailerdecode
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(TRAILER_NO AS INTEGER) AS trailer_no,
        VEHICLEDESCRIPTOR AS vehicledescriptor,
        VINDECODEDON AS vindecodedon,
        TRY_CAST(VINDECODEERROR AS INTEGER) AS vindecodeerror,
        TRY_CAST(VEHICLETYPEID AS INTEGER) AS vehicletypeid,
        VEHICLETYPE AS vehicletype,
        TRY_CAST(MANUFACTURERFULLNAMEID AS INTEGER) AS manufacturerfullnameid,
        MANUFACTURERFULLNAME AS manufacturerfullname,
        TRY_CAST(MAKEID AS INTEGER) AS makeid,
        MAKE AS make,
        TRY_CAST(MODELID AS INTEGER) AS modelid,
        MODEL AS model,
        TRY_CAST(MODELYEAR AS INTEGER) AS modelyear,
        SERIES AS series,
        TRIM AS trim,
        TRY_CAST(PLANTCOUNTRYID AS INTEGER) AS plantcountryid,
        PLANTCOUNTRY AS plantcountry,
        PLANTSTATE AS plantstate,
        PLANTCITY AS plantcity,
        PLANTCOMPANYNAME AS plantcompanyname,
        TRY_CAST(NONLANDUSEID AS INTEGER) AS nonlanduseid,
        NONLANDUSE AS nonlanduse,
        NOTE AS note,
        TRY_CAST(BODYCLASSID AS INTEGER) AS bodyclassid,
        BODYCLASS AS bodyclass,
        TRY_CAST(GROSSVEHICLEWEIGHTRATINGFROMID AS INTEGER) AS grossvehicleweightratingfromid,
        GROSSVEHICLEWEIGHTRATINGFROM AS grossvehicleweightratingfrom,
        TRY_CAST(GROSSVEHICLEWEIGHTRATINGTOID AS INTEGER) AS grossvehicleweightratingtoid,
        GROSSVEHICLEWEIGHTRATINGTO AS grossvehicleweightratingto,
        TRY_CAST(TRAILERBODYTYPEID AS INTEGER) AS trailerbodytypeid,
        TRAILERBODYTYPE AS trailerbodytype,
        TRY_CAST(TRAILERTYPECONNECTIONID AS INTEGER) AS trailertypeconnectionid,
        TRAILERTYPECONNECTION AS trailertypeconnection,
        TRY_CAST(TRAILERLENGTHFT AS DOUBLE) AS trailerlengthft,
        OTHERTRAILERINFO AS othertrailerinfo,
        TRY_CAST(AXLESCOUNT AS INTEGER) AS axlescount,
        TRY_CAST(AXLECONFIGURATIONID AS INTEGER) AS axleconfigurationid,
        AXLECONFIGURATION AS axleconfiguration
FROM bronze.bronze_vpictrailerdecode
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL AND TRAILER_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM analysis.silver_vpictrailerdecode s
            WHERE s.st_case = TRY_CAST(bronze.bronze_vpictrailerdecode.ST_CASE AS INTEGER)
                AND s.veh_no = TRY_CAST(bronze.bronze_vpictrailerdecode.VEH_NO AS INTEGER)
                AND s.trailer_no = TRY_CAST(bronze.bronze_vpictrailerdecode.TRAILER_NO AS INTEGER)
    );
