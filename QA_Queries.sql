--How to validate something does not comply with a taxonomy rule

SELECT
  placement_name
FROM `deg-mmd-data-prd-amg.bqd_analytics.fct_cm360_pm_activity_summary`
WHERE date = "2026-02-01"
  AND NOT REGEXP_CONTAINS(placement_name, r'PID~\d{9}')
