SELECT
  DISTINCT
  REGEXP_EXTRACT(placement_name, r'JP~([^_]+)') AS cjp
FROM `deg-mmd-data-prd-amg.bqd_analytics.fct_cm360_pm_activity_summary`
WHERE date BETWEEN '2026-01-01' AND '2026-02-22'
  AND campaign_id = 34730231
