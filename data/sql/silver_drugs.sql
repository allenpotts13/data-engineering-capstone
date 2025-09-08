CREATE SCHEMA IF NOT EXISTS silver;

INSERT INTO silver.silver_drugs
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(VEH_NO AS INTEGER) AS veh_no,
    TRY_CAST(PER_NO AS INTEGER) AS per_no,
    TRY_CAST(DRUGSPEC AS INTEGER) AS drugspec,
    DRUGSPECNAME AS drugspec_name,
    TRY_CAST(DRUGMETHOD AS INTEGER) AS drugmethod,
    DRUGMETHODNAME AS drugmethod_name,
    TRY_CAST(DRUGRES AS INTEGER) AS drugres,
    DRUGRESNAME AS drugres_name,
    TRY_CAST(DRUGQTY AS INTEGER) AS drugqty,
    DRUGQTYNAME AS drugqty_name,
    TRY_CAST(DRUGACTQTY AS FLOAT) AS drugactqty,
    DRUGACTQTYNAME AS drugactqty_name,
    TRY_CAST(DRUGUOM AS INTEGER) AS druguom,
    DRUGUOMNAME AS druguom_name
FROM bronze.bronze_drugs
WHERE ST_CASE IS NOT NULL AND VEH_NO IS NOT NULL AND PER_NO IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM silver.silver_drugs s
        WHERE s.st_case = TRY_CAST(bronze.bronze_drugs.ST_CASE AS INTEGER)
            AND s.veh_no = TRY_CAST(bronze.bronze_drugs.VEH_NO AS INTEGER)
            AND s.per_no = TRY_CAST(bronze.bronze_drugs.PER_NO AS INTEGER)
    );