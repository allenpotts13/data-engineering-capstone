CREATE SCHEMA IF NOT EXISTS silver;

CREATE OR REPLACE TABLE silver.silver_motorcycle_helmet_laws AS
SELECT
    LOWER(State) AS state,
    LOWER("Required to wear helmet") AS required_to_wear_helmet,
    LOWER("Motorcycle-type vehicles not covered") AS motorcycle_type_vehicles_not_covered,
    LOWER(CAST(Footnotes AS VARCHAR)) AS footnotes
FROM bronze.bronze_motorcycle_helmet_laws
WHERE State IS NOT NULL;
