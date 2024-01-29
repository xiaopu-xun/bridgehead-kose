-- @TD engine_version: 350
-- trial_maihada_basic1(作成時のtmp) を、日毎+各フラグ 集計
WITH day_with AS ( 
  -- 前年同日, 前月同日を付与
  SELECT
    *
    -- 前月同日price
    , LAG(price,      1, 0)    OVER (PARTITION BY day, subscription_flg, first_buy_flg, auto_buy_flg, honpin_flg ORDER BY order_date) AS previous_month_sales
    -- 前月同日day
    , LAG(order_date, 1, NULL) OVER (PARTITION BY day, subscription_flg, first_buy_flg, auto_buy_flg, honpin_flg ORDER BY order_date) AS previous_month_date
    
    -- 前年同日price
    , LAG(price,      1, 0)    OVER (PARTITION BY month, day, subscription_flg, first_buy_flg, auto_buy_flg, honpin_flg ORDER BY order_date) AS previous_year_sales
    -- 前年同日day
    , LAG(order_date, 1, NULL) OVER (PARTITION BY month, day, subscription_flg, first_buy_flg, auto_buy_flg, honpin_flg ORDER BY order_date) AS previous_year_date
  FROM
  (
    -- trial_maihada_basic1(作成時のtmp) を、日毎+各フラグ 集計
    SELECT
      b1.order_date
      , b1.subscription_flg
      , b1.first_buy_flg
      , b1.auto_buy_flg
      , b1.honpin_flg
      , SUM(b1.price * b1.quantity) AS price
      
      -- LAG関数用
      , split_part(order_date,'/',1) as year
      , split_part(order_date,'/',2) as month
      , split_part(split_part(order_date,'/',3),' ',1) as day

    FROM
      tmp_maihada_reporting_basic1 AS b1 -- trial_maihada_basic1 作成時のtmp
    GROUP BY
      b1.order_date
      , b1.subscription_flg
      , b1.first_buy_flg
      , b1.auto_buy_flg
      , b1.honpin_flg
--    ORDER BY 1,2,3,4,5
  )
--  ORDER BY 1,2,3,4,5
)

-- 前月/前年が正しいことを確認して出力
-- 前月(年)データが存在しない場合、前々月(年)を見ているので回避。
SELECT 
  order_date
  , subscription_flg
  , first_buy_flg
  , auto_buy_flg
  , honpin_flg
  , price
  
  -- 前月同日
  -- 一ヶ月以内であることを確認し出力
  , CASE
      WHEN TD_TIME_ADD(TD_TIME_PARSE(order_date,'JST'), '-31d','JST' ) <= TD_TIME_PARSE(previous_month_date,'JST') THEN previous_month_sales
      ELSE 0
    END                                                                                                            AS previous_month_sales
  
  -- 前年同日
  -- 一年以内であることを確認し出力
  , CASE
      WHEN TD_TIME_ADD(TD_TIME_PARSE(order_date,'JST'), '-365d','JST' ) <= TD_TIME_PARSE(previous_year_date,'JST') THEN previous_year_sales
      ELSE 0
    END                                                                                                            AS previous_year_sales

FROM day_with
--ORDER BY 1,2,3,4,5