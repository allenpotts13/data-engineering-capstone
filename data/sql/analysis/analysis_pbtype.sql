CREATE SCHEMA IF NOT EXISTS analysis;

INSERT INTO analysis.silver_pbtype
SELECT
        TRY_CAST(STATE AS INTEGER) AS state,
        STATENAME AS state_name,
        TRY_CAST(ST_CASE AS INTEGER) AS st_case,
        TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
        TRY_CAST(PER_NO AS INTEGER) AS per_no,
        TRY_CAST(PBAGE AS INTEGER) AS pbage,
        PBAGENAME AS pbage_name,
        TRY_CAST(PBSEX AS INTEGER) AS pbsex,
        PBSEXNAME AS pbsex_name,
        TRY_CAST(PBPTYPE AS INTEGER) AS pbptype,
        PBPTYPENAME AS pbptype_name,
        TRY_CAST(PBCWALK AS INTEGER) AS pbcwalk,
        PBCWALKNAME AS pbcwalk_name,
        TRY_CAST(PBSWALK AS INTEGER) AS pbswalk,
        PBSWALKNAME AS pbswalk_name,
        TRY_CAST(PBSZONE AS INTEGER) AS pbszone,
        PBSZONENAME AS pbszone_name,
        TRY_CAST(PEDCTYPE AS INTEGER) AS pedctype,
        PEDCTYPENAME AS pedctype_name,
        TRY_CAST(BIKECTYPE AS INTEGER) AS bikectype,
        BIKECTYPENAME AS bikectype_name,
        TRY_CAST(PEDLOC AS INTEGER) AS pedloc,
        PEDLOCNAME AS pedloc_name,
        TRY_CAST(BIKELOC AS INTEGER) AS bikeloc,
        BIKELOCNAME AS bikeloc_name,
        TRY_CAST(PEDPOS AS INTEGER) AS pedpos,
        PEDPOSNAME AS pedpos_name,
        TRY_CAST(BIKEPOS AS INTEGER) AS bikepos,
        BIKEPOSNAME AS bikepos_name,
        TRY_CAST(PEDDIR AS INTEGER) AS peddir,
        PEDDIRNAME AS peddir_name,
        TRY_CAST(BIKEDIR AS INTEGER) AS bikedir,
        BIKEDIRNAME AS bikedir_name,
        TRY_CAST(MOTDIR AS INTEGER) AS motdir,
        MOTDIRNAME AS motdir_name,
        TRY_CAST(MOTMAN AS INTEGER) AS motman,
        MOTMANNAME AS motman_name,
        TRY_CAST(PEDLEG AS INTEGER) AS pedleg,
        PEDLEGNAME AS pedleg_name,
        TRY_CAST(PEDSNR AS INTEGER) AS pedsnr,
        PEDSNRNAME AS pedsnr_name,
        TRY_CAST(PEDCGP AS INTEGER) AS pedcgp,
        PEDCGPNAME AS pedcgp_name,
        TRY_CAST(BIKECGP AS INTEGER) AS bikecgp,
        BIKECGPNAME AS bikecgp_name
FROM bronze.bronze_pbtype
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
            SELECT 1 FROM analysis.silver_pbtype a
            WHERE a.st_case = TRY_CAST(bronze.bronze_pbtype.ST_CASE AS INTEGER)
                AND a.veh_no = TRY_CAST(bronze.bronze_pbtype.VEH_NO AS INTEGER)
                AND a.per_no = TRY_CAST(bronze.bronze_pbtype.PER_NO AS INTEGER)
    );
