CREATE OR REPLACE VIEW analysis.vw_state_year_summary AS
SELECT
  year,            
  year_num,       
  state,
  state_name,
  COUNT(DISTINCT st_case) AS crashes,
  SUM(fatals)            AS fatalities
FROM analysis.vw_motorcycle_base
GROUP BY year, year_num, state, state_name;