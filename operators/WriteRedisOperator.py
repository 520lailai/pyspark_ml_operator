# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
import json
from tools.RedisUtils import RedisUtils
from tools.JsonUtils import ExtendJSONEncoder
from tools.OperatorsParameterParseUtils import *


"""
    模块功能： 运行用户自定义查询sql
    conf 参数：
         "key": String，存Redis的key
    例子：
    
    1、输入表：
    +---+------+---+------+
    | id|  name|age|   sex|
    +---+------+---+------+
    |  1|lailai| 18|female|
    |  2| guguo| 12|female|
    |  3|  lili| 15|female|
    +---+------+---+------+
    
     2、cof参数配置：
     conf = {"key": "test_redis_key"}
     
"""


class WriteRedisOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        # 1、参数获取
        key = self.conf.get("key")
        # 2、参数检查
        check_parameter_null_or_empty(key, "key", self.op_id)
        check_dataframe(dataframe_list, self.op_id)
        # 3、写Redis
        for dataframe in dataframe_list:
            # 每一个row按照预定义的列顺序排序
            keyorder = dataframe.columns
            rows = []
            for row in dataframe.collect():
                row_dict = row.asDict(True)
                sorted(row_dict.items(), key=lambda i: keyorder.index(i[0]))
                rows.append(row_dict)
            try:
                data = json.dumps(rows, cls=ExtendJSONEncoder)
                RedisUtils.write_redis(key, data)

            except Exception as e:
                e.args += ' op_id :'+ str(self.op_id)
                raise
