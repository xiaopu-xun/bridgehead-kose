-- mediaIdで結合し、キャンペーンごとのmedianameを抽出
WITH media_name AS(
  select
    ads.adId as adId
    ,ads.mediaId
    ,medias.medianame
  from adebis_ads ads
  left join adebis_medias medias
  ON ads.mediaId = medias.mediaId
)
,

-- adIdごとのclick数を算出
click_num AS(
select 
    adId
    ,count(*) as click_num
from adebis_accesses
group by adId
)
,

-- 全データを集約するための一時テーブル作成
itizi AS (
select
    ads.adId
    ,regexp_replace(substring(ads.adstartdate,1,7),'-','/') AS delivery_month
    ,medias2.medianame
    ,cts.click_num
from adebis_ads ads
left join media_name medias2
on ads.adId = medias2.adId
left join click_num cts
on ads.adId = cts.adId
)

-- 一時テーブルからdatatank用に射影
select
  delivery_month as delivery_month
  ,adId as ad_id
  ,medianame as media_name 
  -- ,'test_campaign_name' as campaign_name
  -- ,'test_creative' as creative
  -- ,'test_delivery_num' as delivery_num
  ,click_num as click_num
from itizi
;