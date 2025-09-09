CREATE OR REPLACE VIEW analysis.vw_trend_monthly AS
SELECT
  year,          
  year_num,       
  month,
  COUNT(DISTINCT st_case) AS crashes,
  SUM(fatals)            AS fatalities
FROM analysis.vw_motorcycle_base
GROUP BY year, year_num, month
ORDER BY year_num, month;