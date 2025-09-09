CREATE OR REPLACE VIEW analysis.vw_motorcycle_base AS
SELECT
  a.year                         AS year_num,              
  CAST(a.year AS VARCHAR)        AS year,                  
  a.state,
  a.state_name,                                         
  a.st_case,
  a.month,
  a.hour,
  a.day_week,
  a.day_week_name,
  a.weather,
  a.func_sys                    AS road_class,
  a.wrk_zone                    AS work_zone,
  COALESCE(a.fatals, 0)         AS fatals
FROM analysis.analysis_accident a
JOIN analysis.analysis_vehicle v
  ON a.st_case = v.st_case
WHERE v.body_typ IN (80,81,82,83);