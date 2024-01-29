-- @TD engine_version: 350
--配信月データ取得
--メディアコードが設定されている購買
WITH order_media_code_on_with AS ( 
  SELECT
    o.customer_code_hash
    , o.checkout_timestamp
    , o.order_code
    , o.attributes_media_code
    , o.order_method

  FROM kosedmp_prd_secure.segment_common_order_detail od
    LEFT JOIN kosedmp_prd_secure.segment_common_order o
      ON o.order_code = od.order_code
    LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst eim
      ON eim.item_cd = od.item_code 
    LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AS ar 
      ON ar.customer_code_hash = o.customer_code_hash

  WHERE 
    TD_TIME_RANGE( 
      CAST(checkout_timestamp AS BIGINT) / 1000
    , TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST')
    , TD_TIME_FORMAT(${end_date},   'yyyy-MM-dd', 'JST')
      , 'JST'
    )
    AND eim.category_nm LIKE '%米肌%' 
    AND (ar.system_code = 'F' OR ar.system_code = 'J') 
    AND o.attributes_media_code <> '' 
    AND o.attributes_media_code IS NOT NULL 
  GROUP BY
    o.customer_code_hash
    , o.checkout_timestamp
    , o.order_code
    , o.attributes_media_code
    , o.order_method
)
,

--リピート条件データ取得
--メディアコードが設定されていない購買（トライアルは除く）
order_media_code_off_with AS ( 
  SELECT
    o.customer_code_hash
    , o.checkout_timestamp
    , o.order_code
    , o.attributes_media_code
    , o.order_method

  FROM kosedmp_prd_secure.segment_common_order_detail od
    LEFT JOIN kosedmp_prd_secure.segment_common_order o
      ON o.order_code = od.order_code
    LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst eim
      ON eim.item_cd = od.item_code 
    LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AS ar 
      ON ar.customer_code_hash = o.customer_code_hash

  WHERE 
    TD_TIME_RANGE( 
      CAST(checkout_timestamp AS BIGINT) / 1000
      , TD_TIME_FORMAT(${start_date}, 'yyyy-MM-dd', 'JST')
      , TD_TIME_FORMAT(TD_TIME_ADD(${end_date}, '90d', 'JST'),   'yyyy-MM-dd', 'JST') -- 90日先までのデータを参照
      , 'JST'
    )
    AND eim.category_nm LIKE '%米肌%' 
    AND (ar.system_code = 'F' OR ar.system_code = 'J') 
    AND ( 
      o.attributes_media_code = '' 
      OR o.attributes_media_code IS NULL
    ) 
    AND ( 
      eim.item_no_2 IS NULL 
      OR eim.item_no_2 NOT IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW')
    ) 
    
  GROUP BY
    o.customer_code_hash
    , o.checkout_timestamp
    , o.order_code
    , o.attributes_media_code
    , o.order_method
)
,

-- 「メディアコードが設定されている購買」に対して、90日以内の「メディアコードが設定されていない購買」を取得
order_with AS ( 
  SELECT
    customer_code_hash
    , conversion_order_date
    , attributes_media_code
    , order_method
    , repeat_order_date
    , repeat_num --同じ日付の紐付けカウント
  FROM
    ( 
      SELECT
        omc_on_with.customer_code_hash
        , omc_on_with.checkout_timestamp    AS conversion_order_date
        , omc_on_with.order_method AS order_method
        , omc_off_with.checkout_timestamp   AS repeat_order_date
        , omc_on_with.attributes_media_code AS attributes_media_code
        
        -- リピート回数
        , rank() OVER (PARTITION BY omc_on_with.customer_code_hash, omc_on_with.checkout_timestamp, omc_on_with.attributes_media_code ORDER BY omc_off_with.checkout_timestamp) AS rnk 
        
        -- 同じ日付のリピート紐付けは、1回のみ取りたい(月毎)
        , ROW_NUMBER() OVER (PARTITION BY omc_on_with.customer_code_hash, TD_TIME_FORMAT(CAST(omc_on_with.checkout_timestamp AS BIGINT) / 1000, 'yyyy/MM', 'JST'), omc_off_with.checkout_timestamp, omc_on_with.attributes_media_code) AS repeat_num
      FROM
        order_media_code_on_with omc_on_with 
        LEFT JOIN order_media_code_off_with omc_off_with 
          ON omc_on_with.customer_code_hash = omc_off_with.customer_code_hash 
         AND omc_on_with.checkout_timestamp < omc_off_with.checkout_timestamp 
         AND CAST(omc_off_with.checkout_timestamp AS bigint) / 1000 <= TD_TIME_ADD(CAST(omc_on_with.checkout_timestamp AS bigint) / 1000, '90d', 'JST') -- 90日以内
    ) 
  WHERE
    -- 「メディアコードが設定されている購買」に対して、"2回目購入"のみ取得(リピート(「メディアコードが設定されていない購買」)は最初の1件)。
    rnk = 1 
)
SELECT
    TD_TIME_FORMAT(CAST(conversion_order_date AS BIGINT) / 1000, 'yyyy/MM', 'JST') AS delivery_month
    , CONCAT('メディアコード:', attributes_media_code) AS media_name
    , order_method

      --獲得人数(=レコード数)
    , count(1) AS conversion_num
    
      --リピート人数
      --同じ日のリピートは、1つ目のみ取得
    , count(IF(repeat_num=1, repeat_order_date, NULL)) AS repeat_num

FROM order_with
GROUP BY
  TD_TIME_FORMAT(CAST(conversion_order_date AS BIGINT) / 1000, 'yyyy/MM', 'JST')
  , attributes_media_code
  , order_method