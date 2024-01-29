-- @TD engine_version: 350
-- maisonKOSEのLINE友達を抽出しておく
WITH maison_line_friend AS (
  SELECT
    "uid"
  FROM kosedmp_prd_secure.line_friend
  WHERE client_id = '${client_id}'
),
-- 新規数の集計
new_register_count AS (
  SELECT
    COUNT("uid") AS cnt
  FROM maison_line_friend mlf
  WHERE
    NOT EXISTS(
      SELECT 1
      FROM kosedmp_prd_secure.yesterday_maison_line_friend ymlf
      WHERE ymlf."uid" = mlf."uid"
      AND ymlf.client_id = '${client_id}'
    )
),
-- LINE友達数の集計
line_friend_count AS (
  SELECT
    COUNT("uid") cnt
  FROM maison_line_friend
),
-- LINEID保持登録数の集計
line_retention AS (
  SELECT
    customer_code_hash, sns_line
  FROM kosedmp_prd_secure.segment_common_after_regist scar
  WHERE scar.system_code = 'F'
    AND scar.status = 'VALID'
    AND scar.sns_line <> ''
),
-- LINEID保持登録数の集計
line_retention_count AS (
  SELECT
    COUNT(customer_code_hash) cnt
  FROM line_retention lr
),
-- LINEID保持者の集計
new_line_retention_count AS (
  SELECT
    COUNT(customer_code_hash) AS cnt
  FROM line_retention lr
  WHERE
    NOT EXISTS(
      SELECT 1
      FROM kosedmp_prd_secure.yesterday_maison_line_connect ymlc
      WHERE ymlc.sns_line = lr.sns_line
      AND ymlc.customer_code_hash = lr.customer_code_hash
    )
),
-- 半会員登録者数
-- @TODO 条件が未確定のため、ひとまず0固定としている
half_register_count AS (
  SELECT
    0 AS cnt
)


-- 求めた値の出力
SELECT
    TD_TIME_FORMAT(
      TD_TIME_ADD(
        TD_DATE_TRUNC('day', TD_SCHEDULED_TIME(), 'JST'),
        '-1d',
        'JST'
      ),
      'yyyy/MM/dd',
      'JST'
    ) AS "date"
  , '0' AS new_num --その日追加以外分(全数からその日追加分をマイナス)
  , (line_friend_count.cnt - new_register_count.cnt) AS line_friend_num
  , (line_retention_count.cnt -  new_line_retention_count.cnt) AS line_connect_num
  , half_register_count.cnt AS temporary_membership_num
FROM
    new_register_count
  , line_friend_count
  , line_retention_count
  , new_line_retention_count
  , half_register_count

UNION ALL

SELECT
    TD_TIME_FORMAT(
      TD_TIME_ADD(
        TD_DATE_TRUNC('day', TD_SCHEDULED_TIME(), 'JST'),
        '-1d',
        'JST'
      ),
      'yyyy/MM/dd',
      'JST'
    ) AS "date"
  , '1' AS new_num --その日追加分
  , new_register_count.cnt AS line_friend_num
  , new_line_retention_count.cnt AS line_connect_num
  , half_register_count.cnt AS temporary_membership_num
FROM
    new_register_count
  , line_friend_count
  , line_retention_count
  , new_line_retention_count
  , half_register_count