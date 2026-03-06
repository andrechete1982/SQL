-- Execute the python script to adapt it to be able to upload it into Google Bucket
-- Upload the file onto bucket
-- Create the dataset into raw data / Use V2 as Number of errors allowed / and use 500 max errrors allowed
-- Run the Querie that normalize the file get it ready to be able to replace the clean dataset
DECLARE sql STRING;

SET sql = (
  SELECT
    'CREATE OR REPLACE TABLE `personal-marketing-analytics.Clean_data.confo_conversions_clean_stage` AS SELECT ' ||
    STRING_AGG(
      FORMAT(
        "`%s` AS %s",
        column_name,
        REGEXP_REPLACE(
          LOWER(TRIM(column_name)),
          r'[^a-z0-9]+',
          '_'
        )
      ),
      ', '
    ) ||
    ' FROM `personal-marketing-analytics.raw_dv360.confo_conversions_2`'
  FROM `personal-marketing-analytics.raw_dv360.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'confo_conversions_2'
);

-- Now we delete the existing data from the upload that its already in the clean datatset so it can be replaced after

DELETE FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean`
WHERE date BETWEEN
(
  SELECT MIN(date)
  FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean_stage`
)
AND
(
  SELECT MAX(date)
  FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean_stage`
);

EXECUTE IMMEDIATE sql;

-- No we inserted the new data into from the new upload into the clean dataset - run a min max date 

INSERT INTO `personal-marketing-analytics.Clean_data.confo_conversions_clean`
SELECT *
FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean_stage`;

-- Now we repeat the process of elimnating the exisiting data from the mart table 

DELETE FROM `personal-marketing-analytics.mart.conversions_granular`
WHERE date BETWEEN
(
  SELECT MIN(date)
  FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean_stage`
)
AND
(
  SELECT MAX(date)
  FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean_stage`
);

-- Now we add the incremental data into the mart table

INSERT INTO `personal-marketing-analytics.mart.conversions_granular`

WITH base AS (

SELECT
    date,
    campaign,
    campaign_id,
    activity,
    activity_id,
    placement AS media_buy_name,
    placement_id AS media_buy_key,
    brand_string_ AS brand_ac,
    marsha_code_string_ AS marsha_code,
    mr_prof_member_program_level_string_ AS loyalty_tier,
    checkin_date_string_ AS check_in,
    departure_date_or_check_out_date_string_ AS check_out,
    room_nights_string_ AS rn,
    click_through_conversions AS ctc,
    view_through_conversions  AS vtc,
    click_through_revenue     AS ctr,
    view_through_revenue      AS vtr,
    floodlight_attribution_type AS fatp,
    SAFE_CAST(room_nights_string_ AS FLOAT64) AS room_nights,

    REGEXP_EXTRACT(placement, r'JP~([^_]+)')  AS jp_calc,
    REGEXP_EXTRACT(placement, r'PUB~([^_]+)') AS site_calc,
    REGEXP_EXTRACT(placement, r'CH~([^_]+)')  AS channel_calc,
    REGEXP_EXTRACT(placement, r'MI~([^_]+)')  AS brand_display_calc,
    REGEXP_EXTRACT(placement, r'SL~([^_]+)')  AS special_activation_calc

FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean`

WHERE site_cm360_ != 'APEX OUTCOMES (APEXOUTC)'

AND date BETWEEN
(
  SELECT MIN(date)
  FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean_stage`
)
AND
(
  SELECT MAX(date)
  FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean_stage`
)

),

enriched AS (

SELECT
    b.*,

    jp.description  AS jp,
    pub.description AS site,
    ch.description  AS channel,
    mi.description  AS brand_display,
    sl.description  AS special_activation,

    br.brand AS hotel_brand

FROM base b

LEFT JOIN `personal-marketing-analytics.Clean_data.taxo_dict_clean` jp
ON b.jp_calc = jp.code
AND jp.delimiter = 'JP~'

LEFT JOIN `personal-marketing-analytics.Clean_data.taxo_dict_clean` pub
ON b.site_calc = pub.code
AND pub.delimiter = 'PUB~'

LEFT JOIN `personal-marketing-analytics.Clean_data.taxo_dict_clean` ch
ON b.channel_calc = ch.code
AND ch.delimiter = 'CH~'

LEFT JOIN `personal-marketing-analytics.Clean_data.taxo_dict_clean` mi
ON b.brand_display_calc = mi.code
AND mi.delimiter = 'MI~'

LEFT JOIN `personal-marketing-analytics.Clean_data.taxo_dict_clean` sl
ON b.special_activation_calc = sl.code
AND sl.delimiter = 'SL~'

LEFT JOIN `personal-marketing-analytics.Clean_data.plu_file_clean` br
ON b.marsha_code = br.marsha_code

),

attribution_logic AS (

SELECT
    *,

    CASE
        WHEN jp IN ('Browse','Shop') THEN 0.15
        WHEN jp = 'Buy' THEN 0.07
        ELSE 0
    END AS view_weight,

    COALESCE(ctr,0)
    + COALESCE(vtr,0) *
        CASE
            WHEN jp IN ('Browse','Shop') THEN 0.15
            WHEN jp = 'Buy' THEN 0.07
            ELSE 0
        END
    AS gross_att_revenue,

    COALESCE(ctc,0)
    + COALESCE(vtc,0) *
        CASE
            WHEN jp IN ('Browse','Shop') THEN 0.15
            WHEN jp = 'Buy' THEN 0.07
            ELSE 0
        END
    AS gross_att_bookings,

    CASE
        WHEN fatp = 'Click-through'
        THEN COALESCE(room_nights,0)
        ELSE 0
    END
    +
    CASE
        WHEN fatp = 'View-through'
        THEN COALESCE(room_nights,0)
        ELSE 0
    END *
        CASE
            WHEN jp IN ('Browse','Shop') THEN 0.15
            WHEN jp = 'Buy' THEN 0.07
            ELSE 0
        END
    AS gross_att_room_nights

FROM enriched

),

revenue_rule_applied AS (

SELECT
    a.*,

    ROUND(
        SAFE_DIVIDE(a.gross_att_revenue, NULLIF(a.gross_att_room_nights,0))
    ,2) AS revenue_per_night,

    r.invalid_threshold_rte,
    r.substitute_rte,

    ROUND(
        CASE
            WHEN a.gross_att_room_nights = 0 THEN 0
            WHEN SAFE_DIVIDE(a.gross_att_revenue, NULLIF(a.gross_att_room_nights,0)) > r.invalid_threshold_rte
            THEN r.substitute_rte * a.gross_att_room_nights
            ELSE a.gross_att_revenue
        END
    ,2) AS adjusted_revenue,

    CASE
        WHEN SAFE_DIVIDE(a.gross_att_revenue, NULLIF(a.gross_att_room_nights,0)) > r.invalid_threshold_rte
        THEN TRUE
        ELSE FALSE
    END AS revenue_rule_applied,

  -- Always do a max min date to verify your date range
    CASE
        WHEN a.gross_att_room_nights = 0 THEN 'NO_NIGHTS'
        WHEN SAFE_DIVIDE(a.gross_att_revenue, NULLIF(a.gross_att_room_nights,0)) > r.invalid_threshold_rte
        THEN 'CAPPED'
        ELSE 'VALID'
    END AS revenue_rule_status

FROM attribution_logic a

LEFT JOIN `personal-marketing-analytics.raw_dv360.suspect_revenue_rates` r
ON a.brand_ac = r.brand_cd

)

SELECT *
FROM revenue_rule_applied

