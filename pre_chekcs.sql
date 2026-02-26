--Null Check

SELECT
   COUNT(*) total_rows,
   COUNTIF(site_name IS NULL) null_placement,
   COUNTIF(placement_name IS NULL) null_placememt_name,
   COUNTIF(clickthroughrevenue IS NULL) null_ctr,
   COUNTIF(viewthroughrevenue IS NULL) null_vtr

  FROM `deg-mmd-data-prd-amg.bqd_analytics.fct_cm360_pm_activity_summary`

  WHERE campaign_id = 34730231
    AND floodlightactivity_id = 259820
    AND date BETWEEN '2026-01-01' AND '2026-01-22'
    AND site_name != 'APEX OUTCOMES (APEXOUTC)' 


-- Repeated Values

SELECT
  placement_name,
  date,
  COUNT(*) as c
FROM `deg-mmd-data-prd-amg.bqd_analytics.fct_cm360_pm_activity_summary`
WHERE date BETWEEN '2026-01-01' AND '2026-01-15'
GROUP BY 1,2
HAVING COUNT(*) > 1
