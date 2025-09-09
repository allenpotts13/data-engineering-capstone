CREATE OR REPLACE VIEW analysis.vw_helmet_severity AS
WITH mc_person AS (
  SELECT
    b.year,
    b.year_num,
    b.state,
    b.state_name,
    p.st_case,
    p.veh_no,
    p.per_no,
    COALESCE(
      p.helm_use_name,
      CASE
        WHEN p.helm_use IN (1) THEN 'Yes'
        WHEN p.helm_use IN (0) THEN 'No'
        ELSE 'Unknown'
      END
    ) AS helmet_cat,
    p.inj_sev,
    p.inj_sev_name   
  FROM analysis.analysis_person p
  JOIN analysis.vw_motorcycle_base b ON p.st_case = b.st_case
)
SELECT
  year,
  year_num,
  state,
  state_name,
  helmet_cat,
  inj_sev,
  inj_sev_name,    
  COUNT(*) AS people
FROM mc_person
GROUP BY year, year_num, state, state_name, helmet_cat, inj_sev, inj_sev_name
ORDER BY year_num, helmet_cat, inj_sev;