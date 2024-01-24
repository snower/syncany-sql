
WITH RECURSIVE table_0 AS (
		SELECT 1 AS n
	),
	table_1 AS (
		SELECT n
		FROM table_0
		UNION ALL
		SELECT n + 1
		FROM table_1
		WHERE n < 4
	)
INSERT INTO table_2 SELECT n FROM table_1 table_2;

select n from table_2;