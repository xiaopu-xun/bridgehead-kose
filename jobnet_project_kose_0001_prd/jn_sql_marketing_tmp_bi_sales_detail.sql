/*
sales_detailの情報取得用クエリ
*/


/***********作成した途中テーブルであるprice_date_tmpを利用して、基準日ごとの受注売上および出荷売上を集計するサブクエリ**********/

WITH reference_date AS (

SELECT
	CAST(y1.date_tmp AS DATE) AS date, --日付（比較のためデータ型にする）
	SUBSTRING(y1.date_tmp, 1, 7) AS year_month, --年月
  (CASE WHEN DATE_FORMAT(CAST(y1.date_tmp AS DATE), '%d')
  = DATE_FORMAT(DATE_ADD('MONTH', -1, CAST(y1.date_tmp AS TIMESTAMP)), '%d')
  THEN CAST(SUBSTRING(CAST(DATE_ADD('MONTH', -1, CAST(y1.date_tmp AS TIMESTAMP)) AS VARCHAR), 1, 10) AS DATE)
  ELSE NULL END) AS last_month, --先月(前月で同じ日がない場合、月末日になってしまうので、NULLを入れる)
  (CASE WHEN DATE_FORMAT(CAST(y1.date_tmp AS DATE), '%d')
  = DATE_FORMAT(DATE_ADD('YEAR', -1, CAST(y1.date_tmp AS TIMESTAMP)), '%d')
  THEN CAST(SUBSTRING(CAST(DATE_ADD('YEAR', -1, CAST(y1.date_tmp AS TIMESTAMP)) AS VARCHAR), 1, 10) AS DATE)
  ELSE NULL END) AS last_year, --去年(前年で同じ日がない場合、月末日になってしまうので、NULLを入れる)
	y1.td_system_code, --利用システムコード
	y1.brand, --ブランド
	SUM(CASE
		WHEN y2.price IS NOT NULL
		THEN y2.price
		ELSE 0
		END) AS price_result, --受注売上
	SUM(CASE
		WHEN y3.price IS NOT NULL
		THEN y3.price
		ELSE 0
		END) AS out_sales_amount --出荷売上
FROM
(
select * from
(
	SELECT 'B' AS td_system_code,'K-PAD' AS brand
	  FROM (SELECT 1)
  UNION ALL
	SELECT 'C' AS td_system_code,'Awake' AS brand
	  FROM (SELECT 1)
  UNION ALL
	SELECT 'D' AS td_system_code,'フローラノーティス' AS brand
	  FROM (SELECT 1)
  UNION ALL
	SELECT 'E' AS td_system_code,'ジルスチュアート' AS brand
	  FROM (SELECT 1)
  UNION ALL
	SELECT 'J' AS td_system_code,'米肌' AS brand
	  FROM (SELECT 1)
  UNION ALL
	SELECT 'G' AS td_system_code,'アディクション' AS brand
	  FROM (SELECT 1)
  UNION ALL
	SELECT 'L' AS td_system_code,'DECORTE' AS brand
	  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'Carte' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ONE BY KOSE' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'Others' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'アウェイク' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'アスタブラン' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'アディクション' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'アレルテクト' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'インフィニティ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ヴィセ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ヴィセ アヴァン' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ウルミナプラス' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'エスカラット' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'エスプリーク' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'エスプリーク エクラ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'エルシア' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'オーリック' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'カールキープマジック' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'カルテ クリニティ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'グランデーヌ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'クリアターン' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'クリアプロ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'グレイスワン' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'コエンリッチ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'コスメデコルテ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'コンビニック　セレクテイ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'サンカット' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ジュレーム' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ジルスチュアート' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'スティーブンノル ニューヨーク' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'スポーツビューティー' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'セラミエイド' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'その他（単品）' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ソフティモ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'デルマサージ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ドクターフィル コスメティクス' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ネイチャー アンド　コー' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ネイルホリック' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ハッピーバスディ プレシャスローズ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ビオリス' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ファシオ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'フェリセント' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'フォーチュン' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'フレッシュケア' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'プレディア' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ブレンドベリー' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'フローラノーティス ジルスチュアート' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ポールスチュアート' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ホワイティスト' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'マニフィーク' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'メイクキープミスト' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'メディカラボ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'モイスチュア　エッセンス' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'モイスチュア　スキンリペア' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'モイスチュアマイルド ホワイト' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ラボンヌ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'リップ ジェル マジック' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'ルシェリ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'黒糖精（こくとうせい）' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'潤肌精プライム' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'純肌粋（じゅんきすい）' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'清肌晶（せいきしょう）' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'雪肌精 クリアウェルネス' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'雪肌精（せっきせい）' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'雪肌精エクストラ' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'雪肌精シュープレム' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'雪肌精みやび' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'特別商品' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'肌極（はだきわみ）' AS brand  FROM (SELECT 1)
  UNION ALL
  SELECT 'F' AS td_system_code,'米肌（まいはだ）' AS brand  FROM (SELECT 1)
) as tmp_table1
,
(
    SELECT
    		CAST(dt AS VARCHAR) AS date_tmp
  	FROM
    		(SELECT 1)
  	CROSS JOIN UNNEST(
    		sequence(
      			CAST('${start_date}' AS DATE),
      			CAST('${end_date}' AS DATE),
      			INTERVAL '1' DAY
    		)
  	)
  	AS t(dt)
) as tmp_table2
) y1

LEFT OUTER JOIN
	tmp_bi_checkout_price_date y2
ON
	y1.date_tmp = y2.checkout_date
	AND y1.td_system_code = y2.td_system_code
	AND y1.brand = y2.brand

LEFT OUTER JOIN
	tmp_bi_shipped_price_date  y3
ON
	y1.date_tmp = y3.shipped_date
	AND y1.td_system_code = y3.td_system_code
	AND y2.brand = y3.brand

GROUP BY
	y1.date_tmp,
	y1.td_system_code,
	y1.brand

ORDER BY
	y1.date_tmp ASC,
	y1.td_system_code ASC,
	y1.brand ASC


/******************基準日に沿った各種売上金額の累計値を算出するサブクエリ。目標金額テーブルなどもここで結合する*****************/


), tmp_result AS ( --基準日に沿った各種売上金額を算出するサブクエリ
SELECT
	CAST(t1.date AS VARCHAR) AS date, --日付
	CAST(t1.year_month AS VARCHAR) AS year_month, --年月
	t1.td_system_code, --利用システムコード
	IF(t2.topic IS NULL, '', t2.topic) AS topic, --トピック
	t1.brand, --ブランド
	COALESCE(t3.amount, 0) AS target_sales_amount, --目標売上
	0 AS total_target_sales_amount, --目標売上（累計）
	COALESCE(t1.out_sales_amount, 0) + COALESCE(t4.amount, 0) AS out_sales_amount, --出荷売上（自社EC＋外販）
	COALESCE(t1.out_sales_amount, 0) AS out_sales_amount_ec, --出荷売上自社EC（出荷売上-外販）
	COALESCE(t4.amount, 0) AS out_sales_amount_ex, --出荷売上外販
	0 AS total_out_sales_amount, --出荷売上（累計）
  IF(t1.last_month IS NULL, NULL, COALESCE(t5.out_sales_amount, 0)) AS lm_out_sales_amount, --出荷前月売上
	0 AS total_lm_out_sales_amount, --出荷前月売上（累計）
  IF(t1.last_year IS NULL, NULL, COALESCE(t6.out_sales_amount, 0)) AS ly_out_sales_amount, --出荷前年売上
	0 AS total_ly_out_sales_amount, --出荷前年売上（累計）
	COALESCE(t1.price_result, 0) AS price_result, --受注売上
	0 AS total_price_result, --受注売上（累計）
  IF(t1.last_month IS NULL, NULL, COALESCE(t5.price_result, 0)) AS lm_price_result, --受注前月売上
	0 AS total_lm_price_result, --受注前月売上（累計）
  IF(t1.last_year IS NULL, NULL, COALESCE(t6.price_result, 0)) AS ly_price_result, --受注前年売上
	0 AS total_ly_price_result, --受注前年売上（累計）
	(CASE WHEN t1.price_result > 0 AND t3.amount > 0
	  THEN  ROUND(CAST(t1.price_result AS DOUBLE) / CAST(t3.amount AS DOUBLE), 4)
	 ELSE 0 END) AS price_result_tar_r, --受注売上（目標比）
	(CASE WHEN t1.price_result > 0 AND t5.price_result > 0
	  THEN  ROUND(CAST(t1.price_result AS DOUBLE) / CAST(t5.price_result AS DOUBLE), 4)
	 ELSE 0 END) AS price_result_lm_r, --受注売上（前月比）
	(CASE WHEN t1.price_result > 0 AND t6.price_result > 0
	  THEN  ROUND(CAST(t1.price_result AS DOUBLE) / CAST(t6.price_result AS DOUBLE), 4)
	 ELSE 0 END) AS price_result_ly_r, --受注売上（前年比）
  COALESCE(t1.price_result, 0) - (COALESCE(t1.out_sales_amount, 0) + COALESCE(t4.amount, 0)) AS total_lo_price --残出荷額（累計）
FROM
	reference_date t1

LEFT OUTER JOIN
	bi_segment_topic t2 --基準日と利用システムコードをキーにトピックと結合
ON
	t1.date = CAST(REPLACE(t2.date, '/', '-') AS DATE)
AND
	t1.td_system_code = t2.system_code

LEFT OUTER JOIN --基準日と利用システムコードをキーに目標金額テーブルと結合
	bi_segment_sales_target t3
ON
	t1.date = CAST(REPLACE(t3.date, '/', '-') AS DATE)
AND
	t1.td_system_code = t3.system_code


LEFT OUTER JOIN
	bi_segment_external_shipping_sales t4 --基準日と利用システムコードをキーに出荷売上外販テーブルと結合
ON
	t1.date = CAST(REPLACE(t4.date, '/', '-') AS DATE)
AND
	t1.td_system_code = t4.system_code

LEFT OUTER JOIN--前月までの累計算出用
	reference_date t5
ON
	t1.last_month = t5.date
AND t1.td_system_code = t5.td_system_code
AND t1.brand = t5.brand

LEFT OUTER JOIN --前年までの累計算出用
	reference_date t6
ON
	t1.last_year = t6.date
AND t1.td_system_code = t6.td_system_code
AND t1.brand = t6.brand

/*****************売上を、ブランド別のトータルから、システムコード別のトータルにする。******************/
/*売上(システムコード別) ÷ 購入件数(システムコード別) ＝ 平均注文単価(システムコード別)を計算するため。*/
), price_result_grouping AS (
  SELECT
    date,
    td_system_code,
    SUM(price_result) AS price_result_by_system_code --売上(システムコード別)
  FROM
    tmp_result
  GROUP BY
    date,
    td_system_code
)

/*************************ここからsales_detailテーブルのデータ取得用メインクエリ**************************/

SELECT
	table1.date, --日付（基準日）
	table1.year_month, --年月
	table1.td_system_code as system_cd, --利用システムコード
	table1.topic, --トピック/施策
	table1.brand, --ブランド
	table1.target_sales_amount, --目標売上
	table1.total_target_sales_amount, --目標売上（累計）
	table1.out_sales_amount, --出荷売上
	table1.out_sales_amount_ec, --出荷売上自社EC
	table1.out_sales_amount_ex, --出荷売上外販
	table1.total_out_sales_amount, --出荷売上（累計）
	table1.lm_out_sales_amount, --出荷前月売上
	table1.total_lm_out_sales_amount, --出荷前月売上（累計）
	table1.ly_out_sales_amount, --出荷前年売上
	table1.total_ly_out_sales_amount, --出荷前年売上（累計）
	table1.price_result, --受注売上
	table1.total_price_result, --受注売上（累計）
	table1.lm_price_result, --受注前月売上
	table1.total_lm_price_result, --受注前月売上（累計）
	table1.ly_price_result, --受注前年売上
	table1.total_ly_price_result, --受注前年売上（累計）
	table1.price_result_tar_r, --受注売上（目標比）
	table1.price_result_lm_r, --受注売上（前月比）
	table1.price_result_ly_r, --受注売上（前年比）
	table1.total_lo_price, --残出荷額（累計）
	table2.unique_user, --UU
	table2.page_view, --PV
	table2.session_cnt, --セッション数
	table2.purchases_cnt, --購入件数
	table2.purchases_r, --購入率
	(CASE WHEN table3.price_result_by_system_code > 0 AND table2.purchases_cnt > 0
	  THEN  table3.price_result_by_system_code / table2.purchases_cnt
	 ELSE 0 END) AS avarage_order_unit_price, --平均注文単価
	table2.new_member_cnt, --新規会員数
	table2.new_member_cnt_r, --新規会員登録率
	table2.buyer_cnt, --購入者数
	table2.ex_buyer_r, --既存購入者率
	table2.new_buyer_r, --新規購入者率
 	table2.repeat_r_30, --リピート率30
 	table2.repeat_r_60, --リピート率60
 	table2.repeat_r_90, --リピート率90
 	table2.repeat_r_180 --リピート率180

/*基準日を定義するtable1*/
FROM
	tmp_result table1

/*ユーザーデータとJoin、基準日とtime、利用システムコードを紐付け*/
LEFT OUTER JOIN
	tmp_bi_user_date table2
ON
	table1.date = table2.date
AND
	table1.td_system_code = table2.system_code

/*平均注文単価(システムコード別)とJoin、基準日とtime、利用システムコードを紐付け*/
LEFT OUTER JOIN
	price_result_grouping table3
ON
	table1.date = table3.date
AND
	table1.td_system_code = table3.td_system_code

ORDER BY
	table1.date ASC