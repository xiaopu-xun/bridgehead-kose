SELECT
	sending_id,
	CAST(date AS VARCHAR) AS date,
	name AS name,
	param1 AS param1,
	param2 AS param2,
	param3 AS param3,
	channel AS channel,
	messageparam1 AS messageparam1,
	messageparam2 AS messageparam2,
	messageparam3 AS messageparam3,
	sendingcount,
	extracted,
	messagename AS messagename,
	segmentname AS segmentname,
	messageid AS messageid
FROM
	{database}.fukushima_extract_list_01
EXCEPT
SELECT
	sending_id,
	CAST(date AS VARCHAR) AS date,
	name AS name,
	param1 AS param1,
	param2 AS param2,
	param3 AS param3,
	channel AS channel,
	messageparam1 AS messageparam1,
	messageparam2 AS messageparam2,
	messageparam3 AS messageparam3,
	sendingcount,
	extracted,
	messagename AS messagename,
	segmentname AS segmentname,
	messageid AS messageid
FROM
	{database}.fukushima_previous_extract_list_01
