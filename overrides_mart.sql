CREATE OR REPLACE TABLE `personal-marketing-analytics.mart.conversions_granular_adjusted`
AS
SELECT
m.* EXCEPT(site, jp),

COALESCE(o.override_site, m.site) AS site,
COALESCE(o.override_jp, m.jp) AS jp,

CASE
WHEN o.placement_id IS NOT NULL THEN TRUE
ELSE FALSE
END AS taxonomy_override_applied

FROM `personal-marketing-analytics.mart.conversions_granular` m

LEFT JOIN `personal-marketing-analytics.taxonomy_overrides.list` o
ON m.media_buy_key = o.placement_id
