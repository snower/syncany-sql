# -*- coding: utf-8 -*-
# 2023/4/25
# create by: snower

from syncany.calculaters import register_calculater, TransformCalculater


@register_calculater("transform_row_id")
class RowIdTransformCalculater(TransformCalculater):
    def calculate(self, datas):
        if not datas:
            return datas
        row_id_index, keys = 1, (["row_id"] + list(datas[0].keys()))
        for data in datas:
            data["row_id"] = row_id_index
            row_id_index += 1
        self.update_outputer_schema(keys)
        return datas