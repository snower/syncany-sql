
select pyeval('1 + 2') as a, pyeval('",".join([str(i) for i in range(4)])') as b, pyeval('args[0] + args[1]', 1, 2) as c, pyeval('json.loads(args[0])', '"123"') as d;