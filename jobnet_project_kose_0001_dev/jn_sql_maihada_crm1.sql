-- @TD engine_version: 350
--ユーザごとの購買回数カウント（米肌に限る）
WITH order_num AS ( 
  SELECT
    o.customer_code_hash
    , i.order_code
    , ROW_NUMBER() OVER (PARTITION BY o.customer_code_hash ORDER BY o.checkout_timestamp, o.order_code) AS row_number
    , MIN(o.checkout_timestamp) OVER (PARTITION BY o.customer_code_hash ORDER BY o.checkout_timestamp, o.order_code) AS first_buy_checkout_timestamp 
  FROM
    kosedmp_prd_secure.segment_common_order o
    INNER JOIN ( 
      SELECT
        spod.order_code 
      FROM
        kosedmp_prd_secure.segment_common_order_detail spod 
        LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst eim 
          ON eim.item_cd = spod.item_code 
      WHERE
        eim.category_nm LIKE '%米肌%' 
      GROUP BY
        spod.order_code 
      ORDER BY
        spod.order_code
    ) AS i 
      ON o.order_code = i.order_code
) 
,

--初のトライアル商品購入が何回目の購入か算出
first_trial AS ( 
  SELECT
    first_trial_num.customer_code_hash
    , order_num.order_code
    , first_trial_num.first_num
    , ord.checkout_timestamp 
  FROM
    order_num 
    INNER JOIN ( 
      SELECT
        ord.customer_code_hash
        , MIN(orn.row_number) AS first_num 
      FROM
        kosedmp_prd_secure.segment_common_order AS ord 
        LEFT JOIN kosedmp_prd_secure.segment_common_order_detail AS ordd 
          ON ord.order_code = ordd.order_code 
        LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst AS eim 
          ON ordd.item_code = eim.item_cd 
        LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AS cus 
          ON ord.customer_code_hash = cus.customer_code_hash 
        LEFT JOIN order_num orn 
          ON orn.order_code = ord.order_code 
      WHERE
        (cus.system_code = 'F' OR cus.system_code = 'J') 
        AND eim.item_no_2 IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW') 
      GROUP BY
        ord.customer_code_hash
    ) AS first_trial_num 
      ON first_trial_num.customer_code_hash = order_num.customer_code_hash 
      AND first_trial_num.first_num = row_number 
    LEFT JOIN kosedmp_prd_secure.segment_common_order AS ord 
      ON order_num.order_code = ord.order_code
) 
, 

--初の定期商品購入が何回目の購入か算出
first_subscription AS ( 
  SELECT
    first_trial_num.customer_code_hash
    , order_num.order_code
    , first_trial_num.first_num
    , ord.checkout_timestamp 
  FROM
    order_num 
    INNER JOIN ( 
      SELECT
        ord.customer_code_hash
        , MIN(orn.row_number) AS first_num 
      FROM
        kosedmp_prd_secure.segment_common_order AS ord 
        LEFT JOIN kosedmp_prd_secure.segment_common_order_detail AS ordd 
          ON ord.order_code = ordd.order_code 
        LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst AS eim 
          ON ordd.item_code = eim.item_cd 
        LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AS cus 
          ON ord.customer_code_hash = cus.customer_code_hash 
        LEFT JOIN order_num orn 
          ON orn.order_code = ord.order_code 
      WHERE
        (cus.system_code = 'F' OR cus.system_code = 'J') 
        AND eim.subscription_item = '1' 
      GROUP BY
        ord.customer_code_hash
    ) AS first_trial_num 
      ON first_trial_num.customer_code_hash = order_num.customer_code_hash 
      AND first_trial_num.first_num = row_number 
    LEFT JOIN kosedmp_prd_secure.segment_common_order AS ord 
      ON order_num.order_code = ord.order_code
) 
,

--初の本品購入が何回目の購入か算出
first_honpin AS ( 
  SELECT
    first_trial_num.customer_code_hash
    , order_num.order_code
    , first_trial_num.first_num
    , ord.checkout_timestamp 
  FROM
    order_num 
    INNER JOIN ( 
      SELECT
        ord.customer_code_hash
        , MIN(orn.row_number) AS first_num 
      FROM
        kosedmp_prd_secure.segment_common_order AS ord 
        LEFT JOIN kosedmp_prd_secure.segment_common_order_detail AS ordd 
          ON ord.order_code = ordd.order_code 
        LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst AS eim 
          ON ordd.item_code = eim.item_cd 
        LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AS cus 
          ON ord.customer_code_hash = cus.customer_code_hash 
        LEFT JOIN order_num orn 
          ON orn.order_code = ord.order_code 
      WHERE
        (cus.system_code = 'F' OR cus.system_code = 'J') 
        AND eim.subscription_item != '1' 
        AND ( 
          eim.item_no_2 IS NULL 
          OR eim.item_no_2 NOT IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW')
        ) 
      GROUP BY
        ord.customer_code_hash
    ) AS first_trial_num 
      ON first_trial_num.customer_code_hash = order_num.customer_code_hash 
      AND first_trial_num.first_num = row_number 
    LEFT JOIN kosedmp_prd_secure.segment_common_order AS ord 
      ON order_num.order_code = ord.order_code
),
conversion_rows AS (
SELECT 
  recordid,
  membername,
  amount,
  other1,
  other2,
  other3,
  other4,
  other5,
  userid, 
  accessdate,
  accesstime
FROM kosedmp_prd_secure.adebis_accesses where pageid = 'complete'
),
conversion_adid AS(
SELECT
  recordid,
  membername,
  amount,
  other1,
  other2,
  other3,
  other4,
  other5,
  userid,
  accessdate,
  accesstime,
  adid
FROM (
	SELECT 
  	cr.recordid,
  	cr.membername,
  	cr.amount,
  	cr.other1,
  	cr.other2,
  	cr.other3,
  	cr.other4,
  	cr.other5,
  	cr.userid,
  	cr.accessdate,
  	cr.accesstime,
  	ac.adid,
  	ROW_NUMBER() OVER (PARTITION BY cr.membername ORDER BY ac.accesstime ) AS rownumber
	FROM conversion_rows cr
	JOIN kosedmp_prd_secure.adebis_accesses ac
	on ac.userid = cr.userid and ac.accessdate = cr.accessdate
	where ac.adid <> ''
	and cr.accesstime > ac.accesstime
	)
  WHERE rownumber = 1
),
ad_name AS (
SELECT
 ads.adid,
 gp1.adgroupname as gp1name,
 gp2.adgroupname as gp2name
FROM 
( SELECT adid,mediaid,adgroup1id,adgroup2id 
  FROM kosedmp_prd_secure.tmp_adebis_ads ads 
  GROUP BY adid,mediaid,adgroup1id,adgroup2id
) ads
JOIN kosedmp_prd_secure.tmp_adebis_medias medias
 ON ads.mediaid = medias.mediaid
JOIN kosedmp_prd_secure.tmp_adebis_adgroup1s gp1
 ON ads.adgroup1id = gp1.adgroupid
JOIN kosedmp_prd_secure.tmp_adebis_adgroup2s gp2
 ON ads.adgroup2id = gp2.adgroupid
),
conversion_ad_info AS (
SELECT
 cvad.*,
 an.gp1name,
 an.gp2name
FROM conversion_adid cvad
JOIN ad_name an
ON cvad.adid = an.adid
),
conversion_order AS (
SELECT
od.*,
cai.gp2name as ad_group
FROM kosedmp_prd_secure.segment_common_order od
JOIN conversion_ad_info cai
ON od.order_code = cai.membername
where attributes_media_code = ''
)


SELECT DISTINCT
  TD_TIME_FORMAT( 
    CAST(orn.first_buy_checkout_timestamp AS BIGINT) / 1000, 'yyyy/MM', 'JST') AS first_buy_month
  , co.ad_group AS ad_group
  , ord.customer_code_hash AS customer_code
  , CASE eim.item_no_2
      WHEN 'PRBQ' THEN '肌潤トライアル'
      WHEN 'PRTG' THEN '肌潤トライアル'
      WHEN 'PRBP' THEN '活潤トライアル'
      WHEN 'PRTI' THEN '活潤トライアル'
      WHEN 'PRTT' THEN '美白トライアル'
      WHEN 'PRTU' THEN '美白トライアル'
      WHEN 'PRLB' THEN 'FDトライアル'
      WHEN 'Z5SPROW' THEN 'オイルモニター'
      ELSE ''
    END                                                            AS trial_type
  , ord.order_method AS order_method
  , alea.alea AS abtest_flg
  , IF (eim.subscription_item = '1', 1, 0) AS subscription_flg
  , IF (orn.row_number = 1, 1, 0) AS first_buy_flg

  , CASE eim.item_no_2
      WHEN 'PRBQ' THEN '0'
      WHEN 'PRTG' THEN '0'
      WHEN 'PRBP' THEN '0'
      WHEN 'PRTI' THEN '0'
      WHEN 'PRTT' THEN '0'
      WHEN 'PRTU' THEN '0'
      WHEN 'PRLB' THEN '0'
      WHEN 'Z5SPROW' THEN '0'
      ELSE '1'
    END                                                            AS honpin_flg


  -- 経過日数
  , CASE 
      -- 初回購入＆トライアル -> 0
      WHEN orn.row_number = 1 
           AND eim.item_no_2 IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW')
                                                         THEN 0
      -- 初回購入＆定期 -> 0
      WHEN orn.row_number = 1 
           AND eim.subscription_item = '1'               THEN 0
      
      -- 初回購入＆定期ではない＆トライアル（＝本品）-> 0
      WHEN orn.row_number = 1 
           AND eim.subscription_item != '1' 
           AND ( 
               eim.item_no_2 IS NULL 
               OR eim.item_no_2 NOT IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW')
             )                                           THEN 0
      --
      WHEN f_trial.checkout_timestamp IS NULL            THEN 0
      
      -- トライアル購入あり＆定期商品購入あり -> トライアル購入〜定期商品購入の経過日数
      WHEN f_subscription.checkout_timestamp IS NOT NULL THEN date_diff('day', CAST(TD_TIME_FORMAT(CAST(f_trial.checkout_timestamp AS BIGINT) / 1000, 'yyyy-MM-dd', 'UTC') AS TIMESTAMP) 
                                                                             , CAST(TD_TIME_FORMAT(CAST(f_subscription.checkout_timestamp AS BIGINT) / 1000, 'yyyy-MM-dd', 'UTC') AS TIMESTAMP)) 
      
      -- トライアル購入あり＆本品購入あり ->  トライアル購入〜本品購入の経過日数
      WHEN f_honpin.checkout_timestamp IS NOT NULL       THEN date_diff('day', CAST(TD_TIME_FORMAT(CAST(f_trial.checkout_timestamp AS BIGINT) / 1000, 'yyyy-MM-dd', 'UTC') AS TIMESTAMP) 
                                                                             , CAST(TD_TIME_FORMAT(CAST(f_honpin.checkout_timestamp AS BIGINT) / 1000, 'yyyy-MM-dd', 'UTC') AS TIMESTAMP)) 
      ELSE 0
    END                                                            AS elapsed_days
  
FROM
  kosedmp_prd_secure.segment_common_order AS ord 
  LEFT JOIN kosedmp_prd_secure.segment_common_order_detail AS ordd 
    ON ord.order_code = ordd.order_code 
  LEFT JOIN kosedmp_prd_secure.ecbeing_item_mst AS eim 
    ON ordd.item_code = eim.item_cd 
  LEFT JOIN kosedmp_prd_secure.segment_common_after_regist AS cus 
    ON ord.customer_code_hash = cus.customer_code_hash 
  LEFT JOIN order_num orn 
    ON orn.order_code = ord.order_code 
  LEFT JOIN first_trial AS f_trial 
    ON f_trial.customer_code_hash = ord.customer_code_hash 
  LEFT JOIN first_subscription AS f_subscription 
    ON f_subscription.order_code = ord.order_code 
  LEFT JOIN first_honpin AS f_honpin 
    ON f_honpin.order_code = ord.order_code 
  LEFT JOIN kosedmp_prd_secure.probance_phmdata_client_alea AS alea 
    ON alea.cst_id = ord.customer_code_hash
  LEFT JOIN conversion_order AS co
    ON ord.order_code = co.order_code
WHERE
  (cus.system_code = 'F' OR cus.system_code = 'J') 
  AND eim.category_nm LIKE '%米肌%' 
  AND ( 
    ( 
      orn.row_number = 1 
      AND eim.item_no_2 IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW')
    ) 
    OR ( 
      orn.row_number = 1 
      AND eim.subscription_item = '1'
    ) 
    OR ( 
      orn.row_number = 1 
      AND eim.subscription_item != '1' 
      AND ( 
        eim.item_no_2 IS NULL 
        OR eim.item_no_2 NOT IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW')
      )
    ) 
    OR ( 
      eim.subscription_item = '1' 
      AND f_trial.first_num IS NOT NULL 
      AND f_subscription.first_num IS NOT NULL 
      AND f_trial.first_num < f_subscription.first_num
    ) 
    OR ( 
      eim.subscription_item != '1' 
      AND f_trial.first_num IS NOT NULL 
      AND f_honpin.first_num IS NOT NULL 
      AND ( 
        eim.item_no_2 IS NULL 
        OR eim.item_no_2 NOT IN ('PRBQ', 'PRTG', 'PRBP', 'PRTI', 'PRTT', 'PRTU', 'PRLB', 'Z5SPROW')
      ) 
      AND f_trial.first_num < f_honpin.first_num
    )
  )