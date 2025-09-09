CREATE OR REPLACE VIEW analysis.vw_kpi_overview AS
WITH mc AS (
  SELECT * FROM analysis.vw_motorcycle_base
),
inj AS (
  SELECT
    p.st_case,
    CASE WHEN p.inj_sev IN (3,4) THEN 1 ELSE 0 END AS serious_or_fatal
  FROM analysis.analysis_person p
)
SELECT
  mc.year,            
  mc.year_num,        
  mc.state,
  mc.state_name,
  COUNT(DISTINCT mc.st_case)             AS total_crashes,
  SUM(mc.fatals)                         AS total_fatalities,
  SUM(COALESCE(inj.serious_or_fatal,0))  AS serious_or_fatal_injuries
FROM mc
LEFT JOIN inj ON mc.st_case = inj.st_case
GROUP BY mc.year, mc.year_num, mc.state, mc.state_name;