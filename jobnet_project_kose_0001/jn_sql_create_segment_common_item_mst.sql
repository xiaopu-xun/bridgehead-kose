SELECT
  im.hinmoku_cd
, im.syohin_fg
, im.gosu
, im.hanbai_name_jpn
, im.cut_syohin_name
, im.jan_cd
, im.n_cd
, im.senden_kbn
, im.price
, im.jigyo_class_name
, im.daihan_class_name
, im.hanbai_class_name
, im.line_class_name
, im.category_class_name
, im.func_class1_name
, im.func_class2_name
, im.hatubai_date
, im.limited_kbn
, im.rank_sitei
      FROM item_mst AS im
   INNER JOIN (
      SELECT MAX(time) AS max_time
          , hinmoku_cd
      FROM item_mst
      GROUP BY
          hinmoku_cd
   ) AS new_im
      ON im.time = new_im.max_time
      AND im.hinmoku_cd = new_im.hinmoku_cd;