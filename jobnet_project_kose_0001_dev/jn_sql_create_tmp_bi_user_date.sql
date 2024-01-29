-- @TD engine_version: 350
/*
tmp_bi_user_date --sales_detailと結合するユーザー情報を集計する途中テーブル
期間および利用システムコードは途中テーブルで絞っているので条件なし
*/

SELECT
	t1.date, --日付
	t1.system_code, --利用システムコード
	t1.unique_user, --UU数
	t1.page_view, --PV数
	t1.session_cnt, --セッション数
	t2.purchases_cnt,--購入件数
	ROUND(CAST(t2.purchases_cnt AS DOUBLE) / CAST(t1.session_cnt AS DOUBLE) * 100, 2) AS purchases_r, --購入率
	t2.new_member_cnt, --新規会員数
	ROUND(CAST(t2.new_member_cnt AS DOUBLE) / CAST(t1.unique_user AS DOUBLE) * 100, 2) AS new_member_cnt_r, --新規会員登録率
	t2.buyer_cnt, --購入者数	
	t2.ex_buyer_r, --既存購入者率	
	t2.new_buyer_r, --新規購入者率 
 	t2.repeat_r_30, --リピート率30
 	t2.repeat_r_60, --リピート率60
 	t2.repeat_r_90, --リピート率90
 	t2.repeat_r_180 --リピート率180

FROM
	tmp_bi_session_date t1
LEFT OUTER JOIN
	tmp_bi_purchases_date t2
ON
	t1.date = t2.date
AND
	t1.system_code = t2.system_code

ORDER BY
	t1.date ASC