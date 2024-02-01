
select objectid() as a, objectid('65bb4211eda4fed2e199073e') as b, uuid$uuid4() as c, uuid('54aa0a5c-b54f-4628-8391-3756007d5fc3') as d, snowflakeid() as e, snowflakeid(0) as f;

select random() as a, random$int(0, 10000) as b, random$string(10) as c, random$hexs(10) as d, random$letters(10) as e, random$digits(10) as f, random$prints(10) as g, random$bytes(10) as h;

select random$choice(1, 2, 3, 4) as a;
