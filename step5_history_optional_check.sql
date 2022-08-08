SELECT
	'int' AS `table`,
	count(*) AS `count`,
	max(auto_id) AS `max`
FROM history
UNION
SELECT
	'bigint' AS `table`,
	count(*) AS `count`,
	max(auto_id) AS `max`
FROM history_bigint;