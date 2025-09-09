CREATE OR REPLACE VIEW analysis.vw_weather_crashes AS
SELECT
  year,
  year_num,
  state,
  state_name,
  weather,
  CASE weather
    WHEN 1  THEN 'Clear'
    WHEN 2  THEN 'Rain'
    WHEN 3  THEN 'Sleet or Hail'
    WHEN 4  THEN 'Snow'
    WHEN 5  THEN 'Fog / Smog / Smoke'
    WHEN 6  THEN 'Severe Crosswinds'
    WHEN 7  THEN 'Blowing Sand/Soil/Dirt/Snow'
    WHEN 8  THEN 'Other'
    WHEN 10 THEN 'Cloudy'
    WHEN 11 THEN 'Blowing Snow'
    WHEN 98 THEN 'Not Reported'
    WHEN 99 THEN 'Unknown'
    ELSE 'Uncoded'
  END AS weather_name,
  COUNT(DISTINCT st_case) AS crashes,
  SUM(fatals) AS fatalities
FROM analysis.vw_motorcycle_base
GROUP BY year, year_num, state, state_name, weather, weather_name
ORDER BY year_num, crashes DESC;