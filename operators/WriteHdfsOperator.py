# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *
from RedisUtils import *

""" 
    模块功能： 运行用户自定义查询sql
    conf 参数：
         "file_path": String，写文件的路径（目录）
"""


class WriteHdfsOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        # 1、参数获取
        file_path = self.conf.get("file_path")

        # 2、参数检查
        check_parameter_null_or_empty(file_path, "file_path")
        check_dataframe(dataframe_list)

        # 3、写hdfs
        for dataframe in dataframe_list:
            try:
                dataframe.rdd.repartition(1).toDF().write.mode("overwrite").text(path=file_path)

                dataframe.rdd.repartition(1).toDF().write.mode("overwrite").csv(path=file_path, quote="", sep=" ")
            except WriteHDFSError:
                msg = traceback.format_exc()
                print(msg)


class WriteHDFSError(BaseException):
    def __init__(self, mesg="write hdfs meet error"):
        print(mesg)
