-- @TD engine_version: 350
WITH prep AS(
	SELECT
		IF(cst_id = '', null, cst_id) AS cst_id,
		IF(mem_div = '', null, mem_div) AS mem_div,
		IF(cst_lnm = '', null, cst_lnm) AS cst_lnm,
		IF(cst_fnm = '', null, cst_fnm) AS cst_fnm,
		IF(kn_cst_lnm = '', null, kn_cst_lnm) AS kn_cst_lnm,
		IF(kn_cst_fnm = '', null, kn_cst_fnm) AS kn_cst_fnm,
		IF(seibetsu = '', null, seibetsu) AS seibetsu,
		IF(birth_day = '', null, birth_day) AS birth_day,
		IF(ml_address = '', null, ml_address) AS ml_address,
		IF(phone_ok_div = '', null, phone_ok_div) AS phone_ok_div,
		IF(dm_ok_div = '', null, dm_ok_div) AS dm_ok_div,
		IF(first_buy_div = '', null, first_buy_div) AS first_buy_div,
		IF(quit_div = '', null, quit_div) AS quit_div,
		IF(ent_date = '', null, ent_date) AS ent_date,
		IF(quit_date = '', null, quit_date) AS quit_date,
		IF(last_buy_time = '', null, last_buy_time) AS last_buy_time,
		IF(login_time = '', null, login_time) AS login_time,
		IF(last_acs_time = '', null, last_acs_time) AS last_acs_time,
		IF(post_no01 = '', null, post_no01) AS post_no01,
		IF(post_no02 = '', null, post_no02) AS post_no02,
		IF(todofuken_nm = '', null, todofuken_nm) AS todofuken_nm,
		IF(valid_point = '', null, valid_point) AS valid_point,
		IF(point_edate = '', null, point_edate) AS point_edate,
		IF(system_code = '', null, system_code) AS system_code,
		IF(periodical_buy_div = '', null, periodical_buy_div) AS periodical_buy_div,
		IF(mag_maison = '', null, mag_maison) AS mag_maison,
		IF(mag_maihada = '', null, mag_maihada) AS mag_maihada,
		IF(skin_trouble = '', null, skin_trouble) AS skin_trouble,
		IF(first_buy_time = '', null, first_buy_time) AS first_buy_time,
		buy_count_maison,
		buy_count_maihada,
		IF(mag_drphil = '', null, mag_drphil) AS mag_drphil,
		IF(buy_count_drphil IS NULL, 0, buy_count_drphil) AS buy_count_drphil
	FROM
		kosedmp_dev_secure.probance_data_customer
	EXCEPT
	SELECT
		IF(cst_id = '', null, cst_id) AS cst_id,
		IF(mem_div = '', null, mem_div) AS mem_div,
		IF(cst_lnm = '', null, cst_lnm) AS cst_lnm,
		IF(cst_fnm = '', null, cst_fnm) AS cst_fnm,
		IF(kn_cst_lnm = '', null, kn_cst_lnm) AS kn_cst_lnm,
		IF(kn_cst_fnm = '', null, kn_cst_fnm) AS kn_cst_fnm,
		IF(seibetsu = '', null, seibetsu) AS seibetsu,
		IF(birth_day = '', null, birth_day) AS birth_day,
		IF(ml_address = '', null, ml_address) AS ml_address,
		IF(phone_ok_div = '', null, phone_ok_div) AS phone_ok_div,
		IF(dm_ok_div = '', null, dm_ok_div) AS dm_ok_div,
		IF(first_buy_div = '', null, first_buy_div) AS first_buy_div,
		IF(quit_div = '', null, quit_div) AS quit_div,
		IF(ent_date = '', null, ent_date) AS ent_date,
		IF(quit_date = '', null, quit_date) AS quit_date,
		IF(last_buy_time = '', null, last_buy_time) AS last_buy_time,
		IF(login_time = '', null, login_time) AS login_time,
		IF(last_acs_time = '', null, last_acs_time) AS last_acs_time,
		IF(post_no01 = '', null, post_no01) AS post_no01,
		IF(post_no02 = '', null, post_no02) AS post_no02,
		IF(todofuken_nm = '', null, todofuken_nm) AS todofuken_nm,
		IF(valid_point = '', null, valid_point) AS valid_point,
		IF(point_edate = '', null, point_edate) AS point_edate,
		IF(system_code = '', null, system_code) AS system_code,
		IF(periodical_buy_div = '', null, periodical_buy_div) AS periodical_buy_div,
		IF(mag_maison = '', null, mag_maison) AS mag_maison,
		IF(mag_maihada = '', null, mag_maihada) AS mag_maihada,
		IF(skin_trouble = '', null, skin_trouble) AS skin_trouble,
		IF(first_buy_time = '', null, first_buy_time) AS first_buy_time,
		buy_count_maison,
		buy_count_maihada,
		IF(mag_drphil = '', null, mag_drphil) AS mag_drphil,
		IF(buy_count_drphil IS NULL, 0, buy_count_drphil) AS buy_count_drphil
	FROM
		kosedmp_dev_secure.probance_previous_data_customer
)
-- 個人情報付与
-- 名（カナ）,姓（カナ）,名（漢字）,姓（漢字）,PCメールアドレスが対象
SELECT
	IF(l.cst_id = '', null, l.cst_id) AS cst_id,
	IF(l.mem_div = '', null, l.mem_div) AS mem_div,
	IF(r.last_name = '', null, r.last_name) AS cst_lnm,
	IF(r.first_name = '', null, r.first_name) AS cst_fnm,
	IF(r.last_kana = '', null, r.last_kana) AS kn_cst_lnm,
	IF(r.first_kana = '', null, r.first_kana) AS kn_cst_fnm,
	IF(l.seibetsu = '', null, l.seibetsu) AS seibetsu,
	IF(l.birth_day = '', null, l.birth_day) AS birth_day,
	IF(r.mail_pc = '', null, r.mail_pc) AS ml_address,
	IF(l.phone_ok_div = '', null, l.phone_ok_div) AS phone_ok_div,
	IF(l.dm_ok_div = '', null, l.dm_ok_div) AS dm_ok_div,
	IF(l.first_buy_div = '', null, l.first_buy_div) AS first_buy_div,
	IF(l.quit_div = '', null, l.quit_div) AS quit_div,
	IF(l.ent_date = '', null, l.ent_date) AS ent_date,
	IF(l.quit_date = '', null, l.quit_date) AS quit_date,
	IF(l.last_buy_time = '', null, l.last_buy_time) AS last_buy_time,
	IF(l.login_time = '', null, l.login_time) AS login_time,
	IF(l.last_acs_time = '', null, l.last_acs_time) AS last_acs_time,
	IF(l.post_no01 = '', null, l.post_no01) AS post_no01,
	IF(l.post_no02 = '', null, l.post_no02) AS post_no02,
	IF(l.todofuken_nm = '', null, l.todofuken_nm) AS todofuken_nm,
	IF(l.valid_point = '', null, l.valid_point) AS valid_point,
	IF(l.point_edate = '', null, l.point_edate) AS point_edate,
	IF(l.system_code = '', null, l.system_code) AS system_code,
	IF(l.periodical_buy_div = '', null, l.periodical_buy_div) AS periodical_buy_div,
	IF(l.mag_maison = '', null, l.mag_maison) AS mag_maison,
	IF(l.mag_maihada = '', null, l.mag_maihada) AS mag_maihada,
	IF(l.skin_trouble = '', null, l.skin_trouble) AS skin_trouble,
	IF(l.first_buy_time = '', null, l.first_buy_time) AS first_buy_time,
	IF(l.buy_count_maison = '', null, l.buy_count_maison) AS buy_count_maison,
	IF(l.buy_count_maihada = '', null, l.buy_count_maihada) AS buy_count_maihada,
	IF(l.mag_drphil = '', null, l.mag_drphil) AS mag_drphil,
	IF(buy_count_drphil IS NULL, 0, buy_count_drphil) AS buy_count_drphil
FROM
	prep l
	INNER JOIN ${td.pii_database}.segment_common_after_regist_pii AS r
	ON l.CST_ID = r.customer_code_hash
