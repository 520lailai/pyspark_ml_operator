# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
import redis
from OperatorsUtils import *

''' 
    conf[]：
       host: String,
       port: int
       key：  String  
    dataframe_list:  []
    spark : SparkSession
'''


class WriteRedisOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        host = self.conf["host"]
        port = self.conf["port"]
        key = self.conf["key"]

        check_dataframe(dataframe_list)
        check_str_parameter(host, "the parameter:host is null!")
        check_str_parameter(key, "the parameter:key is null!")
        port = int_convert(port)

        for dataframe in dataframe_list:
            data = dataframe.collect()
            try:
                r = redis.StrictRedis(host=host, port=port)
                r.set(key, data)
            except Exception:
                raise WriteRedisError()


class WriteRedisError(BaseException):
    def __init__(self, mesg="write redis meet error"):
        print(mesg)
