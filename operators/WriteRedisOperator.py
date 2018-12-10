# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *
import json
from RedisUtils import RedisUtils

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
        check_parameter_null_or_empty(key, "key")
        check_dataframe(dataframe_list)

        # 3、写Redis
        for dataframe in dataframe_list:
            data = json.dumps(dataframe.collect())
            RedisUtils.write_redis(key, data)
