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
	IF(buy_count_drphil IS NULL, 0, buy_count_drphil) AS buy_count_drphil,
  IF(customer_rank = '', null, customer_rank) AS customer_rank
FROM
	probance_data_customer
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
	IF(buy_count_drphil IS NULL, 0, buy_count_drphil) AS buy_count_drphil,
  IF(customer_rank = '', null, customer_rank) AS customer_rank
FROM
	probance_previous_data_customer