CREATE OR REPLACE TABLE `personal-marketing-analytics.Clean_data.taxonomy_qa_issues` AS

WITH media_source AS (

SELECT
campaign,
campaign_id,
paid_search_engine_account,
advertiser,
placement_id,
placement
FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean`
WHERE paid_search_advertiser IS NULL
  AND advertiser = 'AN~Marriott_RE~NA'

),

-- Split tokens
tokens AS (

SELECT
ms.campaign,
ms.campaign_id,
ms.paid_search_engine_account,
ms.advertiser,
ms.placement_id,
ms.placement,
token

FROM media_source ms,
UNNEST(SPLIT(ms.placement,'|')) AS token

),

-- Extract delimiter and code
parsed AS (

SELECT
t.campaign,
t.campaign_id,
t.paid_search_engine_account,
t.advertiser,
t.placement_id,
t.placement,
t.token,
REGEXP_EXTRACT(t.token,r'^[A-Z]+~') AS delimiter,
REGEXP_EXTRACT(t.token,r'~(.+)$') AS code

FROM tokens t

WHERE REGEXP_EXTRACT(t.token,r'^[A-Z]+~') != 'ID~'
  AND REGEXP_EXTRACT(t.token,r'^[A-Z]+~') != 'PD~'

),

-- Validate against dictionary
validated AS (

SELECT
p.campaign,
p.campaign_id,
p.paid_search_engine_account,
p.advertiser,
p.placement_id,
p.placement,
p.token,
p.delimiter,
p.code,
d.description

FROM parsed p

LEFT JOIN `personal-marketing-analytics.Clean_data.taxo_dict_clean` d
ON p.delimiter = d.delimiter
AND p.code = d.code

),

-- Identify issues
issues AS (

SELECT
v.campaign,
v.campaign_id,
v.paid_search_engine_account,
v.advertiser,
v.placement_id,
v.placement,
v.token,
v.delimiter,
v.code,

CASE
WHEN v.delimiter IS NULL THEN 'invalid_token_format'
WHEN v.description IS NULL THEN 'invalid_code'
ELSE NULL
END AS issue_type

FROM validated v

)

SELECT *
FROM issues
WHERE issue_type IS NOT NULL
