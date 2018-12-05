# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
import redis
from OperatorsUtils import *
import ConfigParser
import json

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
        # host = self.conf["host"]
        # port = self.conf["port"]
        key = self.conf["key"]
        try:
            config = ConfigParser.RawConfigParser()
            config.read("framework/core/conf/common.conf")
            host = config.get("redis", "redis_host")
            port = config.getInt("redis", "redis_port")
        except Exception:
            host = "bjpg-rs2856.yz02"
            port = "14330"
        app_name = self.conf["app_name"]
        opid = self.conf["opid"]

        key = app_name+opid

        check_dataframe(dataframe_list)
        check_str_parameter(host, "the parameter:host is null!")
        check_str_parameter(key, "the parameter:key is null!")
        port = int_convert(port)

        for dataframe in dataframe_list:
            data = json.dumps(dataframe.collect())
            try:
                r = redis.StrictRedis(host=host, port=port)
                r.set(key, data)
            except Exception:
                raise WriteRedisError()


class WriteRedisError(BaseException):
    def __init__(self, mesg="write redis meet error"):
        print(mesg)
