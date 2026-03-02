--We are missing just to add the RPN rule and match it with the Brand Classf file 

WITH base AS (

  SELECT
    date,
    placement AS media_buy_name,
    placement_id AS media_buy_key, 

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
    sl.description  AS special_activation

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

),

attribution_logic AS (

  SELECT
    *,

    -- Weight logic (centralized)
    CASE
      WHEN jp IN ('Browse','Shop','Buy') THEN 0.15
      ELSE 0
    END AS view_weight,

    -- Attributed Revenue
    COALESCE(ctr,0)
    +
    COALESCE(vtr,0) *
      CASE
        WHEN jp IN ('Browse','Shop','Buy') THEN 0.15
        ELSE 0
      END
    AS att_revenue,

    -- Attributed Bookings
    COALESCE(ctc,0)
    +
    COALESCE(vtc,0) *
      CASE
        WHEN jp IN ('Browse','Shop','Buy') THEN 0.15
        ELSE 0
      END
    AS att_bookings,

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
        WHEN jp IN ('Browse','Shop','Buy') THEN 0.15
        ELSE 0
      END
    AS att_room_nights

  FROM enriched

)

SELECT *
FROM attribution_logic;
