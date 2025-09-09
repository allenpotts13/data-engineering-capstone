CREATE OR REPLACE VIEW analysis.vw_impairment_rate AS
WITH mc_veh AS (
  SELECT DISTINCT
    a.year, a.year_num,
    a.state, a.state_name,
    v.st_case, v.veh_no
  FROM analysis.vw_motorcycle_base a
  JOIN analysis.analysis_vehicle v ON a.st_case = v.st_case
),
imp AS (
  SELECT
    st_case, veh_no,
    CASE
      WHEN drimpair IS NULL THEN 0
      WHEN TRY_CAST(drimpair AS INTEGER) = 0 THEN 0
      ELSE 1
    END AS impaired
  FROM analysis.analysis_drimpair
)
SELECT
  m.year, m.year_num,
  m.state, m.state_name,
  COUNT(*)                              AS motorcycles,
  SUM(COALESCE(imp.impaired,0))         AS impaired_involved,
  1.0 * SUM(COALESCE(imp.impaired,0)) / NULLIF(COUNT(*),0) AS impairment_rate
FROM mc_veh m
LEFT JOIN imp
  ON m.st_case = imp.st_case AND m.veh_no = imp.veh_no
GROUP BY m.year, m.year_num, m.state, m.state_name
ORDER BY m.year_num, m.state;