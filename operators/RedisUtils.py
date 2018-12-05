# -*- coding: utf-8 -*-
import redis
import ConfigParser
import json
import traceback

''' 
    conf[]：
       project_name : String
       opid : String
    dataframe_list:  []
    spark : SparkSession
'''

host = None
port = None
try:
    config = ConfigParser.RawConfigParser()
    config.read("framework/core/conf/common.conf")
    host = config.get("redis", "redis_host")
    port = int(config.getInt("redis", "redis_port"))
except Exception:
    host = "bjpg-rs2856.yz02"
    port = 14330

class RedisUtils:
    def write_redis(key, value):
        try:
            r = redis.StrictRedis(host=host, port=port)
            r.set(key, value)
            print("set to redis:", r.get(key))
        except Exception:
            msg = traceback.format_exc()
            print(msg)
            raise WriteRedisError()


class WriteRedisError(BaseException):
    def __init__(self, mesg="write redis meet error"):
        print(mesg)
