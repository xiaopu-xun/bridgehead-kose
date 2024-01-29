-- @TD engine_version: 350
/*受注*/

WITH tmp AS (

SELECT 
	system_cd,
	brand,
	SUM(target_sales_amount) AS total_tar
FROM
 	tmp_bi_sales_detail
WHERE
		DATE_ADD('day', -(DAY(CURRENT_TIMESTAMP)), CURRENT_TIMESTAMP) <= CAST(date AS DATE)
	AND
		CAST(CURRENT_TIMESTAMP AS DATE) >= CAST(date AS DATE)
GROUP BY 
	system_cd,
	brand

)


SELECT
	CAST(CAST(CURRENT_TIMESTAMP AS DATE) AS VARCHAR)  as date,
	t1.system_cd,
	t1.brand,
	t2.total_tar,
	SUM(aaa) as total_price,
	SUM(yyy) as total_lm_price,
	SUM(zzz) as total_ly_price
	
FROM
	(
	SELECT 
		system_cd,
		brand,
		0 as xxx,
		0 as yyy, 
		0 as zzz,
		SUM(price_result) AS aaa
	FROM
   		tmp_bi_sales_detail
	WHERE
		DATE_ADD('day', -(DAY(CURRENT_TIMESTAMP)), CURRENT_TIMESTAMP) <= CAST(date AS DATE)
	AND
		CAST(CURRENT_TIMESTAMP AS DATE) >= CAST(date AS DATE)
	GROUP BY
		system_cd,
		brand

	UNION ALL
	
	SELECT
		system_cd,
		brand,
		0 as xxx,
		0 as yyy, 
		SUM(price_result) AS yyy, 
		0 as aaa
	FROM
    		tmp_bi_sales_detail
	WHERE
		DATE_ADD('MONTH', -1, DATE_ADD('day', -(DAY(CURRENT_TIMESTAMP)), CURRENT_TIMESTAMP)) <= CAST(date AS DATE)
	AND
		DATE_ADD('MONTH', -1, CAST(CURRENT_TIMESTAMP AS DATE)) >= CAST(date AS DATE)
	GROUP BY
		system_cd,
		brand
		
	UNION ALL
	SELECT
		system_cd,
		brand,
		0 as xxx,
		SUM(price_result) as zzz, 
		0 AS zzz, 
		0 as aaa
	FROM
   		 tmp_bi_sales_detail
	WHERE
		DATE_ADD('YEAR', -1, DATE_ADD('day', -(DAY(CURRENT_TIMESTAMP)), CURRENT_TIMESTAMP)) <= CAST(date AS DATE)
	AND
		DATE_ADD('YEAR', -1, CAST(CURRENT_TIMESTAMP AS DATE)) >= CAST(date AS DATE)
	GROUP BY
		system_cd,
		brand
	) AS t1

LEFT OUTER JOIN
	tmp t2

ON
	t1.system_cd = t2.system_cd
AND
	t1.brand=t2.brand

GROUP BY
	t1.system_cd,
	t1.brand,
	t2.total_tar