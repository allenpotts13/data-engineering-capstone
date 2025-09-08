INSERT INTO analysis.analysis_weather
SELECT
    TRY_CAST(STATE AS INTEGER) AS state,
    STATENAME AS state_name,
    TRY_CAST(ST_CASE AS INTEGER) AS st_case,
    TRY_CAST(WEATHER AS INTEGER) AS weather,
    WEATHERNAME AS weather_name
FROM bronze.bronze_weather
WHERE ST_CASE IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM analysis.analysis_weather a
      WHERE a.st_case = TRY_CAST(bronze.bronze_weather.ST_CASE AS INTEGER)
  );
