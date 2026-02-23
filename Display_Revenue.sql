SELECT
  site_name AS site,

  FORMAT("%'.2f", SUM(clickthroughrevenue)) AS CTR,

  FORMAT("%'.2f", SUM(viewthroughrevenue)) AS VTR,

  FORMAT("%'.2f",
    SUM(clickthroughrevenue)
    +
    SUM(
      viewthroughrevenue *
      CASE
        WHEN REGEXP_CONTAINS(LOWER(placement_name), r'(book|buy)') THEN 0.07
        WHEN REGEXP_CONTAINS(LOWER(placement_name), r'(browse|shop)') THEN 0.15
        ELSE 0
      END
    )
  ) AS attributed_revenue,

  MAX(lastupdated) AS max_date

FROM `deg-mmd-data-prd-amg.bqd_analytics.fct_cm360_pm_activity_summary`

WHERE campaign_id = 34730231
  AND floodlightactivity_id = 259820
  AND date BETWEEN '2026-01-01' AND '2026-02-22'

GROUP BY site_name
