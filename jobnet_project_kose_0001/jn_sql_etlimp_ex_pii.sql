SELECT
    customer_code_hash,
    status,
    sex,
    birthday,
    lang,
    zip_code,
    country,
    state,
    currency_code,
    timezone,
    system_code,
    pris_merge_code_hash,
    pris_permissioncode,
    kpad_gatheringcode,
    kpad_shop_id,
    customer_code_2,
    customer_code_3,
    customer_code_4,
    customer_code_5,
    jillappcustomercode,
    mail_pc_hash,
    mail_mobile_hash,
    ablemail,
    sns_facebook,
    sns_twitter,
    sns_google,
    sns_yahoo,
    sns_line,
    amazon,
    linefriend,
    kpadclientclass,
    ecrank,
    calltime,
    clubkosedelete,
    clubkosedeletedate,
    systemcreatedate,
    systemupdatedate,
    affiliate,
    publishid,
    ip,
    browser,
    ecsight,
    mobileuid,
    guid,
    device,
    buytimes_shop,
    buytimes_ec,
    buytimes_ec_cancel,
    buytimes_ec_return,
    mailmagazine_jill,
    mailmagazine_flora,
    mailmagazine_maison,
    mailmagazine_maihada,
    mailmagazine_awake,
    mailmagazine_addiction,
    filecreatedate,
    periodical_active_flag,
    able_tel_flag,
    state_flag,
    mailmagazine_drphil,
    month_of_birth,
    memberscard_id,
    mailmagazine_mall,
    mailmagazine_sekkisei,
    customerrank_maihada,
    skin_troubles_maihada,
    mailmagazine_decorte,
    skin_troubles_decorte,
    skin_type_decorte,
    customerrank_decorte,
    kose_id_createdate
FROM
    ${td.pii_database}.customer_pii
WHERE
    TD_TIME_RANGE(
        time,
        TD_TIME_FORMAT(TD_SCHEDULED_TIME(), 'yyyy-MM-dd', 'JST'),
        TD_TIME_FORMAT(TD_TIME_ADD(TD_SCHEDULED_TIME(), '1d', 'UTC'), 'yyyy-MM-dd', 'JST'),
        'JST'
    )