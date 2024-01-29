WITH order_1st AS ( -- 初回購入情報
	SELECT
  		customer_code_hash,
  		order_code,
      checkout_date,
      customer_create_date
  	FROM
  		(
  		SELECT
    			customer_code_hash,
    			order_code,
          checkout_date,
          customer_create_date,
    			ROW_NUMBER() OVER (PARTITION BY customer_code_hash ORDER BY checkout_timestamp) AS row_number
  		FROM
			bi_segment_order
    		WHERE
			checkout_timestamp <> ''
    		AND
			checkout_timestamp IS NOT NULL
  		)
  	WHERE row_number = 1
), order_2nd AS ( -- ２つ目購入情報
	SELECT
  		customer_code_hash,
  		order_code,
      checkout_date
  	FROM
  		(
  		SELECT
    			customer_code_hash,
    			order_code,
          checkout_date,
    			ROW_NUMBER() OVER (PARTITION BY customer_code_hash ORDER BY checkout_timestamp) AS row_number
  		FROM
			bi_segment_order
    		WHERE
			checkout_timestamp <> ''
    		AND
			checkout_timestamp IS NOT NULL
  		)
  	WHERE row_number = 2
)

SELECT
	bso.checkout_date AS date, --日付（キー情報）
	bso.system_code, --利用システムコード（キー情報）
	COUNT(bso.customer_code_hash) AS purchases_cnt,--購入件数
	SUM(CASE WHEN o1.checkout_date = o1.customer_create_date THEN 1 ELSE 0 END) AS new_member_cnt, --新規会員数
	COUNT(DISTINCT bso.customer_code_hash) AS buyer_cnt, --購入者数
	ROUND(100 - CAST(COUNT(DISTINCT o1.customer_code_hash) AS DOUBLE)
		/ CAST(COUNT(DISTINCT bso.customer_code_hash) AS DOUBLE) * 100, 2) AS ex_buyer_r, --既存購入者率
	ROUND(CAST(COUNT(DISTINCT o1.customer_code_hash) AS DOUBLE)
		/ CAST(COUNT(DISTINCT bso.customer_code_hash) AS DOUBLE) * 100, 2) AS new_buyer_r, --新規購入者率
  ROUND(CAST(COUNT(DISTINCT o2.customer_code_hash) AS DOUBLE)
    / CAST(COUNT(DISTINCT bso.customer_code_hash) AS DOUBLE) * 100, 2) AS repeat_r, --２回目購入の割合（リピート率全て）
  ROUND(CAST(COUNT(DISTINCT o2_r30.customer_code_hash) AS DOUBLE)
    / CAST(COUNT(DISTINCT o1_r.customer_code_hash) AS DOUBLE) * 100, 2) AS repeat_r_30, --２回目購入、かつ初回～２回目が30日以内の割合（リピート率30）
  ROUND(CAST(COUNT(DISTINCT o2_r60.customer_code_hash) AS DOUBLE)
    / CAST(COUNT(DISTINCT o1_r.customer_code_hash) AS DOUBLE) * 100, 2) AS repeat_r_60, --２回目購入、かつ初回～２回目が60日以内の割合（リピート率60）
  ROUND(CAST(COUNT(DISTINCT o2_r90.customer_code_hash) AS DOUBLE)
    / CAST(COUNT(DISTINCT o1_r.customer_code_hash) AS DOUBLE) * 100, 2) AS repeat_r_90, --２回目購入、かつ初回～２回目が90日以内の割合（リピート率90）
  ROUND(CAST(COUNT(DISTINCT o2_r180.customer_code_hash) AS DOUBLE)
    / CAST(COUNT(DISTINCT o1_r.customer_code_hash) AS DOUBLE) * 100, 2) AS repeat_r_180 --２回目購入、かつ初回～２回目が180日以内の割合（リピート率180）

FROM
	bi_segment_order bso -- 全購入データを顧客コードでDISTINCTしたテーブル

/*==================================================================================*/
/* 既存購入者比率・新規購入者比率・２回目購入の割合（リピート率全て）を算出 */

LEFT OUTER JOIN
	order_1st o1 -- 初回購入データ
ON -- 全購入データと同じ顧客で同じ日だった場合、この初回購入データをLEFT JOINする
  bso.customer_code_hash = o1.customer_code_hash
AND
  bso.checkout_date = o1.checkout_date
LEFT OUTER JOIN
	order_2nd o2 -- ２回目購入データ
ON -- 全購入データと同じ顧客で同じ日だった場合、この２回目購入データをLEFT JOINする
	bso.customer_code_hash = o2.customer_code_hash
AND
  bso.checkout_date = o2.checkout_date

/*==================================================================================*/
/* ２回目購入、かつ初回～２回目が30,60,90,180日以内の各割合（リピート率30,60,90,180） */

LEFT OUTER JOIN
	order_1st o1_r -- 初回購入データ
ON -- 全購入データと同じ顧客だった場合、この初回購入データをLEFT JOINする
  bso.customer_code_hash = o1_r.customer_code_hash
LEFT OUTER JOIN
	order_2nd o2_r30 -- ２回目購入データ(30日以内用)
ON -- 全購入データと同じ顧客で同じ日で、初回購入～２回目購入が30日以内の場合、この２回目購入データをLEFT JOINする
	bso.customer_code_hash = o2_r30.customer_code_hash
AND
  bso.checkout_date = o2_r30.checkout_date
AND
	CAST(o2_r30.checkout_date AS DATE) <=
	CAST(DATE_ADD('DAY', 30, CAST(o1_r.checkout_date AS TIMESTAMP)) AS DATE)
LEFT OUTER JOIN
	order_2nd o2_r60 -- ２回目購入データ(60日以内用)
ON -- 全購入データと同じ顧客で同じ日で、初回購入～２回目購入が60日以内の場合、この２回目購入データをLEFT JOINする
	bso.customer_code_hash = o2_r60.customer_code_hash
AND
  bso.checkout_date = o2_r60.checkout_date
AND
	CAST(o2_r60.checkout_date AS DATE) <=
	CAST(DATE_ADD('DAY', 60, CAST(o1_r.checkout_date AS TIMESTAMP)) AS DATE)
LEFT OUTER JOIN
	order_2nd o2_r90 -- ２回目購入データ(90日以内用)
ON -- 全購入データと同じ顧客で同じ日で、初回購入～２回目購入が90日以内の場合、この２回目購入データをLEFT JOINする
	bso.customer_code_hash = o2_r90.customer_code_hash
AND
  bso.checkout_date = o2_r90.checkout_date
AND
	CAST(o2_r90.checkout_date AS DATE) <=
	CAST(DATE_ADD('DAY', 90, CAST(o1_r.checkout_date AS TIMESTAMP)) AS DATE)
LEFT OUTER JOIN
	order_2nd o2_r180 -- ２回目購入データ(180日以内用)
ON -- 全購入データと同じ顧客で同じ日で、初回購入～２回目購入が180日以内の場合、この２回目購入データをLEFT JOINする
	bso.customer_code_hash = o2_r180.customer_code_hash
AND
  bso.checkout_date = o2_r180.checkout_date
AND
	CAST(o2_r180.checkout_date AS DATE) <=
	CAST(DATE_ADD('DAY', 180, CAST(o1_r.checkout_date AS TIMESTAMP)) AS DATE)

GROUP BY
	bso.checkout_date,
	bso.system_code
ORDER BY
	bso.checkout_date,
	bso.system_code