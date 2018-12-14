# -*- coding: utf-8 -*-
import redis
import ConfigParser
import traceback
import os


class RedisUtils:
    try:
        config = ConfigParser.RawConfigParser()
        config.read("conf/common.conf")
        __host = config.get("redis", "redis_host")
        __port = int(config.get("redis", "redis_port"))
    except IOError:
        __host = "bjpg-rs2856.yz02"
        __port = 14330

    __redis_client = redis.StrictRedis(host=__host, port=__port)

    @staticmethod
    def write_redis(key, value):
        try:
            RedisUtils.__redis_client.set(key, value)
            print("set to redis:", RedisUtils.__redis_client.get(key))
        except Exception:
            msg = traceback.format_exc()
            print(msg)
            raise WriteRedisError()

    @staticmethod
    def read_redis(key):
        try:
            data = RedisUtils.__redis_client.get(key)
            print("get to redis:", data)
        except Exception:
            msg = traceback.format_exc()
            print(msg)
            raise ReadRedisError()
        return data

    @staticmethod
    def save_file(key, conf):
        data_key = conf.get("data_key")
        file_path = conf.get("file_path")
        if not key:
            raise ValueError("the conf parameter:key ")

        if not os.path.exists(file_path):
            raise ValueError("the file_path is not exist:" + file_path)

        files = []
        if os.path.isdir(file_path):
            files = os.listdir(file_path)
        elif os.path.isfile(file_path):
            files.append(file_path)

        for afile in files:
            try:
                f = open(afile, 'r')
                data = f.read()
                RedisUtils.write_redis(key,{data_key:data})
            finally:
                if f:
                    f.close()


if __name__ == "__main__":
    RedisUtils.write_redis("test_redis_key1",
                           '[[1, "US", 15, 10.0], [2, "CA", 12, 10.0], [3, "CA", 7, 50.0], [4, "CA", 7, 17.0], [5, "NZ", 15, 10.0]]')
    print(RedisUtils.read_redis("test_redis_key1"))


class WriteRedisError(BaseException):
    def __init__(self, mesg="write redis meet error"):
        print(mesg)


class ReadRedisError(BaseException):
    def __init__(self, mesg="write redis meet error"):
        print(mesg)
