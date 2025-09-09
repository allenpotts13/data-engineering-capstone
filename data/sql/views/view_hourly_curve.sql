CREATE OR REPLACE VIEW analysis.vw_hourly_curve AS
SELECT
  year, year_num,
  hour,
  COUNT(DISTINCT st_case) AS crashes
FROM analysis.vw_motorcycle_base
GROUP BY year, year_num, hour
ORDER BY year_num, hour;