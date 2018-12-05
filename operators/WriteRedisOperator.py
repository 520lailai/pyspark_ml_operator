# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *
import json
from RedisUtils import RedisUtils

''' 
    conf[]：
       project_name : String
       opid : String
    dataframe_list:  []
    spark : SparkSession
'''


class WriteRedisOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        project_name = self.conf["project_name"]
        opid = self.conf["opid"]
        check_str_parameter(project_name, "the parameter:project_name is null!")
        check_str_parameter(opid, "the parameter:opid is null!")

        key = "arthur"+project_name + "-" + opid

        check_dataframe(dataframe_list)
        check_str_parameter(key, "the parameter:key is null!")

        for dataframe in dataframe_list:
            data = json.dumps(dataframe.collect())
            RedisUtils().write_redis(key, data)
