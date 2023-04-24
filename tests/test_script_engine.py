# -*- coding: utf-8 -*-
# 2023/4/23
# create by: snower

from unittest import TestCase
from syncanysql import ScriptEngine


class ScriptEngineTestCase(TestCase):
    def get_test_data(self):
        return [
            {"site_id": 8, "site_name": "黄豆网", "site_amount": 17.04, "timeout_at": "16:00:00",
             "vip_timeout_at": "11:00:00"},
            {"site_id": 9, "site_name": "青菜网", "site_amount": 7.2, "timeout_at": "15:00:00",
             "vip_timeout_at": "11:00:00"},
            {"site_id": 8, "site_name": "去啥网", "site_amount": 10.41, "timeout_at": "16:00:00",
             "vip_timeout_at": "11:00:00"},
            {"site_id": 10, "site_name": "汽车网", "site_amount": 5.8, "timeout_at": "16:00:00",
             "vip_timeout_at": "11:00:00"},
            {"site_id": 9, "site_name": "火箭网", "site_amount": 4.5, "timeout_at": "15:00:00",
             "vip_timeout_at": "10:00:00"},
            {"site_id": 9, "site_name": "卫星网", "site_amount": 11.2, "timeout_at": "16:40:00",
             "vip_timeout_at": "11:20:00"},
        ]

    def get_stats_data(self):
        return [
            {'site_id': 8, 'site_name': '黄豆网', 'order_cnt': 2, 'total_amount': 27.45},
            {'site_id': 9, 'site_name': '青菜网', 'order_cnt': 3, 'total_amount': 22.9},
            {'site_id': 10, 'site_name': '汽车网', 'order_cnt': 1, 'total_amount': 5.8}
        ]

    def test_script_engine(self):
        with ScriptEngine() as engine:
            engine.push_memory_datas("test_data", self.get_test_data())
            self.assertEqual(engine.get_memory_datas("test_data"), self.get_test_data())

            engine.execute('''
                INSERT INTO `stats_data` SELECT site_id, site_name, count(*) order_cnt, sum(site_amount) total_amount 
                FROM test_data GROUP BY site_id;
            ''')
            self.assertEqual(engine.get_memory_datas("stats_data"), self.get_stats_data())
            self.assertEqual(engine.pop_memory_datas("stats_data"), self.get_stats_data())
            self.assertEqual(engine.get_memory_datas("stats_data"), [])

            engine.use("test_func", lambda: 1)
            engine.set_variable("test_var", 1)

            with engine.context() as context:
                context.use("test_func", lambda: 2)
                context.set_variable("test_var", 2)
                context.use("test_context_func", lambda: 2)
                context.set_variable("test_context_var", 2)

                context.execute('''
                    insert into `result_data` select test_func() as test_func, @test_var as test_var,
                     test_context_func() as test_context_func, @test_context_var as test_context_var;
                     
                     set global @aaa=1;
                ''')
                self.assertEqual(context.get_memory_datas("result_data"),
                                 [{"test_func": 2, "test_var": 2, "test_context_func": 2, "test_context_var": 2}])
                self.assertEqual(context.pop_memory_datas("result_data"),
                                 [{"test_func": 2, "test_var": 2, "test_context_func": 2, "test_context_var": 2}])
                self.assertEqual(context.get_memory_datas("result_data"), [])

            engine.execute('''
                insert into `result_data` select test_func() as test_func, @test_var as test_var;
                                
                insert into `result_aaa` select @aaa as aaa;
            ''')
            self.assertEqual(engine.get_memory_datas("result_data"),
                             [{"test_func": 1, "test_var": 1}])
            self.assertEqual(engine.pop_memory_datas("result_data"),
                             [{"test_func": 1, "test_var": 1}])
            self.assertEqual(engine.get_memory_datas("test_context_data"), [])

            self.assertEqual(engine.pop_memory_datas("result_aaa"), [{"aaa": 1}])

