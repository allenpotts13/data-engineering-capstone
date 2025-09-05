CREATE OR REPLACE TABLE silver_motorcycle_helmet_laws AS
SELECT
    LOWER(state) AS state,
    LOWER("required to wear helmet") AS required_to_wear_helmet,
    LOWER("motorcycle-type vehicles not covered") AS motorcycle_type_vehicles_not_covered,
    LOWER(footnotes) AS footnotes
FROM bronze_motorcycle_helmet_laws
WHERE state IS NOT NULL;
