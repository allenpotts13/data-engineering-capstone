CREATE OR REPLACE TABLE silver_weather AS
SELECT
    TRY_CAST(state AS INTEGER) AS state,
    LOWER(statename) AS statename,
    TRY_CAST(st_case AS INTEGER) AS st_case,
    TRY_CAST(weather AS INTEGER) AS weather,
    LOWER(weathername) AS weather_name
FROM bronze_weather
WHERE st_case IS NOT NULL;
