-- トライアル/ 本品 それぞれの初回購入を取得
WITH first_order_with AS (
  SELECT
      customer_code_hash
    , checkout_timestamp
    , item_no_2
    , honpin_or_trial
    , subscription_item
    , order_count

      --初回受注発生場所
    , FIRST_VALUE(order_method) OVER(PARTITION BY customer_code_hash ORDER BY order_count) AS first_order_method

      --初回購入日
      --order_count でsortすると、トライアル・本品の同時購入時にトライアルが後に来るかも。
    , FIRST_VALUE(checkout_timestamp) OVER (PARTITION BY customer_code_hash ORDER BY checkout_timestamp, order_count_hort) AS first_checkout_timestamp

      --初回購入発送日
      --order_count でsortすると、トライアル・本品の同時購入時にトライアルが後に来るかも。
    , FIRST_VALUE(shipped_timestamp) OVER (PARTITION BY customer_code_hash ORDER BY shipped_timestamp, order_count_hort) AS first_shipped_timestamp

      --ユーザごとの購買回数カウント:同order_codeでも、本品がトライアルの後に来るように。
    , ROW_NUMBER() OVER (PARTITION BY customer_code_hash ORDER BY checkout_timestamp, item_no_2 desc, order_count_hort) AS order_number
  FROM
  (
    SELECT
        *
        --トライアル商品/ 本品（定期含む）それぞれの購入カウント
        , ROW_NUMBER() OVER (PARTITION BY customer_code_hash, honpin_or_trial ORDER BY checkout_timestamp, order_code, order_detail_code) AS order_count_hort
    FROM
    (
        SELECT
          o.customer_code_hash
          , o.checkout_timestamp
          , CASE WHEN  o.shipped_timestamp = '' AND o.checkout_timestamp < '1573138800000' THEN o.checkout_timestamp ELSE o.shipped_timestamp END as shipped_timestamp
            --- 移行データ（2019/11/08以前）かつshipped_timestamが空の場合はcheckouttimestampにする。11/8以降のオーダーは発送されていない場合があるためそのまま空。
          , o.order_code
          , od.order_detail_code

            --本品/トライアル種別
          , eim.item_no_2
          , CASE WHEN eim.item_no_2 IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW') THEN 0 ELSE 1 END AS honpin_or_trial -- 0:trial, 1:honpin

            --定期フラグ用
          , eim.subscription_item

           --受注発生場所
          , o.order_method

            --ユーザごとの購買回数カウント:order_code毎
          , rank() OVER (PARTITION BY o.customer_code_hash ORDER BY o.checkout_timestamp, o.order_code) AS order_count

        FROM segment_common_order_detail od
          LEFT JOIN segment_common_order o
            ON o.order_code = od.order_code
          LEFT JOIN ecbeing_item_mst eim
            ON eim.item_cd = od.item_code
          LEFT JOIN segment_common_after_regist AS ar
            ON ar.customer_code_hash = o.customer_code_hash

        WHERE eim.category_nm LIKE '%米肌%'
          AND (ar.system_code = 'F' OR ar.system_code = 'J')
        GROUP BY
            o.customer_code_hash
          , o.checkout_timestamp
          , o.shipped_timestamp
          , o.order_code
          , od.order_detail_code
          , eim.item_no_2
          , eim.subscription_item
          , o.order_method
    )
    where shipped_timestamp <> '' --未発送は対象外
  )
  WHERE
  -- 初回購入のみ取得
  order_count_hort = 1
)

SELECT
  TD_TIME_FORMAT(CAST(fow.checkout_timestamp AS BIGINT) / 1000, 'yyyy/MM', 'JST') AS order_month
  , ''                                     AS ad_group
  , fow.customer_code_hash                 AS customer_code

    --トライアル種別
  , CASE fow.item_no_2
      WHEN 'PRBQ' THEN '肌潤トライアル'
      WHEN 'PRTG' THEN '肌潤トライアル'
      WHEN 'PRBP' THEN '活潤トライアル'
      WHEN 'PRTI' THEN '活潤トライアル'
      WHEN 'PRTT' THEN '美白トライアル'
      WHEN 'PRTU' THEN '美白トライアル'
      WHEN 'PRLB' THEN 'FDトライアル'
      WHEN 'Z5SPROW' THEN 'オイルモニター'
      ELSE ''
    END                                    AS trial_type

    --初回受注発生場所
  , fow.first_order_method                 AS first_order_method

  , alea.alea                              AS abtest_flg
  , IF (fow.subscription_item = '1', 1, 0) AS subscription_flg

    --新規フラグ
    --トライアル・本品同時購入時は両方フラグが立つ
  , CASE
      WHEN fow.order_count = 1             THEN 1
      ELSE 0
    END                                    AS first_buy_flg

    -- 0:trial, 1:honpin
  , fow.honpin_or_trial                    AS honpin_flg


  -- リピート経過フラグ
  -- トライアルかつ初回→本品（定期含む）の本品レコードに算出
  , CASE
      -- トライアルの場合は0固定
      WHEN fow.honpin_or_trial <> 1        THEN '0'

      WHEN fow.order_number = 1            THEN '0'

      -- トライアル購入後30日以内=1
      WHEN date_diff('day', CAST(TD_TIME_FORMAT(CAST(first_shipped_timestamp AS BIGINT) / 1000, 'yyyy-MM-dd', 'JST') AS TIMESTAMP)
                          , CAST(TD_TIME_FORMAT(CAST(checkout_timestamp       AS BIGINT) / 1000, 'yyyy-MM-dd', 'JST') AS TIMESTAMP)
           ) <= 30                         THEN '1'

      -- トライアル購入後60日以内=2
      WHEN date_diff('day', CAST(TD_TIME_FORMAT(CAST(first_shipped_timestamp AS BIGINT) / 1000, 'yyyy-MM-dd', 'JST') AS TIMESTAMP)
                          , CAST(TD_TIME_FORMAT(CAST(checkout_timestamp       AS BIGINT) / 1000, 'yyyy-MM-dd', 'JST') AS TIMESTAMP)
           ) <= 60                         THEN '2'

      -- トライアル購入後90日以内=3
      WHEN date_diff('day', CAST(TD_TIME_FORMAT(CAST(first_shipped_timestamp AS BIGINT) / 1000, 'yyyy-MM-dd', 'JST') AS TIMESTAMP)
                          , CAST(TD_TIME_FORMAT(CAST(checkout_timestamp       AS BIGINT) / 1000, 'yyyy-MM-dd', 'JST') AS TIMESTAMP)
           ) <= 90                         THEN '3'

      ELSE '0'
    END                                    AS repeat_type
FROM
  first_order_with fow
  LEFT JOIN probance_phmdata_client_alea AS alea
    ON alea.cst_id = fow.customer_code_hash