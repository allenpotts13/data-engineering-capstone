CREATE OR REPLACE VIEW analysis.vw_speeding_rate AS
WITH mc_veh AS (
  SELECT
    a.year                           AS year_num,               
    CAST(a.year AS VARCHAR)          AS year,                    
    a.state,
    a.state_name,
    v.st_case,
    v.veh_no,
    CASE
      WHEN TRY_CAST(v.speedrel AS INTEGER) IS NULL
        OR TRY_CAST(v.speedrel AS INTEGER) = 0
      THEN 0 ELSE 1
    END AS speeding
  FROM analysis.analysis_vehicle v
  JOIN analysis.analysis_accident a
    ON v.st_case = a.st_case
  WHERE v.body_typ IN (80,81,82,83) 
)
SELECT
  year,
  year_num,
  state,
  state_name,
  COUNT(*)                                        AS motorcycles,
  SUM(speeding)                                   AS speeding_involved,
  1.0 * SUM(speeding) / NULLIF(COUNT(*), 0)       AS speeding_rate
FROM mc_veh
GROUP BY year, year_num, state, state_name
ORDER BY year_num, speeding_rate DESC;