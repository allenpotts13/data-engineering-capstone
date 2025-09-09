CREATE OR REPLACE VIEW analysis.vw_roadclass_crashes AS
WITH base AS (
  SELECT
    year,
    year_num,
    state,
    state_name,
    road_class,   
    st_case
  FROM analysis.vw_motorcycle_base
),
labeled AS (
  SELECT
    year,
    year_num,
    state,
    state_name,
    road_class,
    CASE road_class
      WHEN 1 THEN 'Interstate'
      WHEN 2 THEN 'Principal Arterial – Freeway/Expressway'
      WHEN 3 THEN 'Principal Arterial – Other'
      WHEN 4 THEN 'Minor Arterial'
      WHEN 5 THEN 'Major Collector'
      WHEN 6 THEN 'Minor Collector'
      WHEN 7 THEN 'Local'
      ELSE 'Unknown/Other'
    END AS road_class_name,
    st_case
  FROM base
)
SELECT
  year,
  year_num,
  state,
  state_name,
  road_class,
  road_class_name,
  COUNT(DISTINCT st_case) AS crashes
FROM labeled
GROUP BY year, year_num, state, state_name, road_class, road_class_name
ORDER BY year_num, crashes DESC;