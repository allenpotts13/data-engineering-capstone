CREATE OR REPLACE VIEW analysis.vw_model_vehicles AS
WITH m AS (
  SELECT
    b.year,
    b.year_num,
    b.state,
    b.state_name,
    v.st_case,
    v.veh_no,
    COALESCE(
      NULLIF(TRIM(v.vpicmodelname), ''),
      NULLIF(TRIM(v.model), ''),
      NULLIF(TRIM(v.mak_mod), ''),
      'Unknown'
    ) AS model_name
  FROM analysis.vw_motorcycle_base b
  JOIN analysis.analysis_vehicle v
    ON b.st_case = v.st_case
  WHERE v.body_typ IN (80,81,82,83)
)
SELECT
  year, year_num, state, state_name, model_name,
  COUNT(*)                AS motorcycles,
  COUNT(DISTINCT st_case) AS crashes
FROM m
GROUP BY year, year_num, state, state_name, model_name;