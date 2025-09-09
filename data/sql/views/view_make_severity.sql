CREATE OR REPLACE VIEW analysis.vw_make_severity AS
WITH mp AS (
  SELECT
    b.year, b.year_num, b.state, b.state_name,
    p.st_case, p.veh_no, p.per_no,
    COALESCE(
      NULLIF(TRIM(v.vpicmakename), ''),
      NULLIF(TRIM(v.makename), ''),
      'Unknown'
    ) AS make_name,
    p.inj_sev,
    p.inj_sev_name
  FROM analysis.vw_motorcycle_base b
  JOIN analysis.analysis_vehicle v
    ON b.st_case = v.st_case AND v.body_typ IN (80,81,82,83)
  JOIN analysis.analysis_person p
    ON p.st_case = v.st_case AND p.veh_no = v.veh_no
)
SELECT
  year, year_num, state, state_name,
  make_name,
  inj_sev, inj_sev_name,
  COUNT(*) AS people
FROM mp
GROUP BY year, year_num, state, state_name, make_name, inj_sev, inj_sev_name;