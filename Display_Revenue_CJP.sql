 WITH base AS ( 
  SELECT
    site_name,
    clickthroughrevenue,
    viewthroughrevenue,
    
    CASE
      WHEN REGEXP_CONTAINS(LOWER(placement_name), r'(book|buy)') THEN 'Buy'
      WHEN REGEXP_CONTAINS(lower(placement_name), r'(browse)') THEN 'Browse'
      WHEN REGEXP_CONTAINS(lower(placement_name), r'(shop)') THEN 'Shop'
      Else 'Unclassified'
    END AS campaing_journey_phase

  FROM `deg-mmd-data-prd-amg.bqd_analytics.fct_cm360_pm_activity_summary`

  WHERE campaign_id = 34730231
    AND floodlightactivity_id = 259820
    AND date BETWEEN '2026-01-01' AND '2026-02-22'
 )

SELECT
  site_name AS site,
  campaing_journey_phase,

  FORMAT("%'.2f", SUM(clickthroughrevenue)) AS CTR,

  FORMAT("%'.2f", SUM(viewthroughrevenue)) AS VTR,

  FORMAT("%'.2f",
    SUM(clickthroughrevenue)
    +
    SUM(
      viewthroughrevenue *
      CASE
        WHEN campaing_journey_phase = 'Browse' THEN 0.15
        WHEN campaing_journey_phase = 'Shop' THEN 0.15
        WHEN campaing_journey_phase = 'Buy' THEN 0.07
        ELSE 0
      END
    )
  ) AS attributed_revenue

FROM base

GROUP BY 1,2

