
SET @j = '{"a": 1, "b": 2, "c": {"d": 4}}';
SET @j2 = '1';
SELECT JSON_CONTAINS(@j, @j2, '$.a');
SELECT JSON_CONTAINS(@j, @j2, '$.b');
SET @j2 = '{"d": 4}';
SELECT JSON_CONTAINS(@j, @j2, '$.a');
SELECT JSON_CONTAINS(@j, @j2, '$.c');

SET @j = '{"a": 1, "b": 2, "c": {"d": 4}}';
SELECT JSON_CONTAINS_PATH(@j, 'one', '$.a', '$.e');
SELECT JSON_CONTAINS_PATH(@j, 'all', '$.a', '$.e');
SELECT JSON_CONTAINS_PATH(@j, 'one', '$.c.d');
SELECT JSON_CONTAINS_PATH(@j, 'one', '$.a.d');

SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]');
SELECT JSON_EXTRACT('[10, 20, [30, 40]]', '$[2][*]');
SELECT JSON_EXTRACT('[10, 20, [{"a":30}, {"b":40}]]', '$[2][*]["a"]');

SELECT JSON_DEPTH('{}'), JSON_DEPTH('[]'), JSON_DEPTH('true');
SELECT JSON_DEPTH('[10, 20]'), JSON_DEPTH('[[], {}]');
SELECT JSON_DEPTH('[10, {"a": 20}]');

SELECT JSON_KEYS('{"a": 1, "b": {"c": 30}}');
SELECT JSON_KEYS('{"a": 1, "b": {"c": 30}}', '$.b');

SELECT JSON_LENGTH('[1, 2, {"a": 3}]');
SELECT JSON_LENGTH('{"a": 1, "b": {"c": 30}}');
SELECT JSON_LENGTH('{"a": 1, "b": {"c": 30}}', '$.b');

SELECT JSON_VALID('{"a": 1}');
SELECT JSON_VALID('hello'), JSON_VALID('"hello"');

SET @j = '{"a": 1, "b": 2, "c": {"d": 4}}';
SELECT JSON_SET(@j, '$.a', 2), JSON_SET(@j, '$.c.d', 2);
SELECT JSON_SET('"1"', '$[0]', 'a'), JSON_SET('"1"', '$[2]', 'a');
SELECT JSON_SET('["1"]', '$[0]', 'a'), JSON_SET('["1"]', '$[2]', 'a');

SELECT JSON_REMOVE(@j, '$.a', '$.c.d'), JSON_REMOVE(@j, '$.c.a');
SELECT JSON_REMOVE('"1"', '$[0]'), JSON_REMOVE('"1"', '$[2]');
SELECT JSON_REMOVE('["1"]', '$[0]'), JSON_REMOVE('["1"]', '$[2]');