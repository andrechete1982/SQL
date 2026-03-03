CREATE OR REPLACE TABLE `personal-marketing-analytics.mart.conversions_granular`
PARTITION BY date
CLUSTER BY marsha_code, media_buy_key
AS

WITH base AS (

  SELECT
    date,
    campaign, #Campaign friendly name
    campaign_id, #Campaign id
    activity, #Floodlight Activity
    activity_id, #Floodlight Activity ID
    placement AS media_buy_name,
    placement_id AS media_buy_key,
    marsha_code_string_ AS marsha_code,
    mr_prof_member_program_level_string_ AS loyalty_tier,
    checkin_date_string_ AS check_in,
    departure_date_or_check_out_date_string_ as check_out,  

    room_nights_string_ AS rn,
    click_through_conversions AS ctc,
    view_through_conversions  AS vtc,
    click_through_revenue     AS ctr,
    view_through_revenue      AS vtr,

    floodlight_attribution_type AS fatp,

    -- Cast once here (important)
    SAFE_CAST(room_nights_string_ AS FLOAT64) AS room_nights,

    REGEXP_EXTRACT(placement, r'JP~([^_]+)')  AS jp_calc,
    REGEXP_EXTRACT(placement, r'PUB~([^_]+)') AS site_calc,
    REGEXP_EXTRACT(placement, r'CH~([^_]+)')  AS channel_calc,
    REGEXP_EXTRACT(placement, r'MI~([^_]+)')  AS brand_display_calc,
    REGEXP_EXTRACT(placement, r'SL~([^_]+)')  AS special_activation_calc

  FROM `personal-marketing-analytics.Clean_data.confo_conversions_clean`
  WHERE campaign_id = 34730231
    AND date BETWEEN '2026-02-01' AND '2026-02-28'
    AND site_cm360_ != 'APEX OUTCOMES (APEXOUTC)'

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

  -- Taxonomy Joins 

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

  -- Marsha Dimension Joins

  LEFT JOIN `personal-marketing-analytics.Clean_data.plu_file_clean` br
    ON b.marsha_code = br.marsha_code

),

attribution_logic AS (

SELECT
    *,

    -- Centralized weight logic
    CASE
      WHEN jp IN ('Browse','Shop') THEN 0.15
      WHEN jp = 'Buy' THEN 0.07
      ELSE 0
    END AS view_weight,

    -- Attributed Revenue
    COALESCE(ctr,0)
      + COALESCE(vtr,0) *
        CASE
          WHEN jp IN ('Browse','Shop') THEN 0.15
          WHEN jp = 'Buy' THEN 0.07
          ELSE 0
        END
      AS gross_att_revenue,

    -- Attributed Bookings
    COALESCE(ctc,0)
      + COALESCE(vtc,0) *
        CASE
          WHEN jp IN ('Browse','Shop') THEN 0.15
          WHEN jp = 'Buy' THEN 0.07
          ELSE 0
        END
      AS gross_att_bookings,

    -- Attributed Room Nights
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
    END
      *
        CASE
          WHEN jp IN ('Browse','Shop') THEN 0.15
          WHEN jp = 'Buy' THEN 0.07
          ELSE 0
        END
    AS gross_att_room_nights

FROM enriched

)

SELECT *
FROM attribution_logic;
