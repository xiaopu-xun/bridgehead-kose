SELECT
    DISTINCT
      newest_cust.customer_code_hash
    , newest_cust.status
    , newest_cust.sex
    , newest_cust.birthday
    , newest_cust.lang
    , newest_cust.zip_code
    , newest_cust.country
    , newest_cust.state
    , newest_cust.currency_code
    , newest_cust.timezone
    , newest_cust.system_code
    , newest_cust.pris_merge_code_hash
    , newest_cust.pris_permissioncode
    , newest_cust.kpad_gatheringcode
    , newest_cust.kpad_shop_id
    , newest_cust.customer_code_2
    , newest_cust.customer_code_3
    , newest_cust.customer_code_4
    , newest_cust.customer_code_5
    , newest_cust.jillappcustomercode
    , newest_cust.mail_pc_hash
    , newest_cust.mail_mobile_hash
    , newest_cust.ablemail
    , newest_cust.sns_facebook
    , newest_cust.sns_twitter
    , newest_cust.sns_google
    , newest_cust.sns_yahoo
    , newest_cust.sns_line
    , newest_cust.amazon
    , newest_cust.linefriend
    , newest_cust.kpadclientclass
    , newest_cust.ecrank
    , newest_cust.calltime
    , newest_cust.clubkosedelete
    , newest_cust.clubkosedeletedate
    , newest_cust.systemcreatedate
    , newest_cust.systemupdatedate
    , newest_cust.affiliate
    , newest_cust.publishid
    , newest_cust.ip
    , newest_cust.browser
    , newest_cust.ecsight
    , newest_cust.mobileuid
    , newest_cust.guid
    , newest_cust.device
    , newest_cust.buytimes_shop
    , newest_cust.buytimes_ec
    , newest_cust.buytimes_ec_cancel
    , newest_cust.buytimes_ec_return
    , newest_cust.mailmagazine_jill
    , newest_cust.mailmagazine_flora
    , newest_cust.mailmagazine_maison
    , newest_cust.mailmagazine_maihada
    , newest_cust.mailmagazine_awake
    , newest_cust.mailmagazine_addiction
    , newest_cust.filecreatedate
    , newest_cust.periodical_active_flag
    , newest_cust.able_tel_flag
    , newest_cust.state_flag
    , newest_cust.mailmagazine_drphil_dist AS mailmagazine_drphil
    , newest_cust.month_of_birth
    , newest_cust.memberscard_id
    , newest_cust.mailmagazine_mall_dist AS mailmagazine_mall
    , newest_cust.mailmagazine_sekkisei_dist AS mailmagazine_sekkisei
    , newest_cust.customerrank_maihada
    , newest_cust.skin_troubles_maihada
FROM
  (
    SELECT
        dist_cust.*
      , CASE
            WHEN dist_cust.mailmagazine_drphil IS NULL THEN '0'
            WHEN dist_cust.mailmagazine_drphil = '' THEN '0'
            ELSE dist_cust.mailmagazine_drphil
          END AS mailmagazine_drphil_dist
      , CASE
            WHEN dist_cust.mailmagazine_mall IS NULL THEN '0'
            WHEN dist_cust.mailmagazine_mall = '' THEN '0'
            ELSE dist_cust.mailmagazine_mall
          END AS mailmagazine_mall_dist
      , CASE
            WHEN dist_cust.mailmagazine_sekkisei IS NULL THEN '0'
            WHEN dist_cust.mailmagazine_sekkisei = '' THEN '0'
            ELSE dist_cust.mailmagazine_sekkisei
          END AS mailmagazine_sekkisei_dist
    FROM
      (
        SELECT
            cust.*
        FROM customer AS cust
        INNER JOIN
          (
            SELECT
                customer_code_hash
              , filecreatedate
              , systemupdatedate
              , mailmagazine_drphil
              , mailmagazine_mall
              , mailmagazine_sekkisei
              , RANK() OVER (PARTITION BY
                                 customer_code_hash
                             ORDER BY
                                 filecreatedate DESC
                               , systemupdatedate DESC
                            ) AS rnk
            FROM customer
          ) AS new_cust
          ON  cust.filecreatedate = new_cust.filecreatedate
          AND cust.systemupdatedate = new_cust.systemupdatedate
          AND cust.customer_code_hash = new_cust.customer_code_hash
        WHERE new_cust.rnk = 1
      ) AS dist_cust
  ) AS newest_cust