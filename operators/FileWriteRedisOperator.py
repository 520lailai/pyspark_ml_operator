# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from RedisUtils import RedisUtils
import os

''' 
    conf[]：
       project_name : String
       opid : String
       file_path: string
'''


class FileWriteRedisOperator(DataProcessingOperator):

    def handle(self, dataframe_list=None, spark=None):
        project_name = self.conf["project_name"]
        opid = self.conf["opid"]
        key = "arthur" + project_name + "-" + str(opid)
        file_path = self.conf["file_path"]

        if os.path.exists(file_path):
            raise ValueError("the file_path is not exist:" + file_path)

        files = []
        if os.path.isdir(file_path):
            files = os.listdir(file_path)
        elif os.path.isfile(file_path):
            files.append(file_path)

        for afile in files:
            try:
                file = open(afile, 'r')
                data = file.read()
                RedisUtils.write_redis(key, data)
            finally:
                if file:
                    file.close()
