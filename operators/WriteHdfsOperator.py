# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *
import traceback


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
        check_parameter_null_or_empty(file_path, "file_path", self.op_id)
        check_dataframe(dataframe_list, self.op_id)

        # 3、写hdfs
        for dataframe in dataframe_list:
            types = dataframe.dtypes
            for type in types:
                if type[1] == 'vector' or type[1] == list:
                    dataframe = dataframe.withColumn(type[0], dataframe[type[0]].cast("string"))
            try:
                dataframe.rdd.repartition(1).toDF().write.mode("overwrite").csv(path=file_path, quote="", sep=" ")

            except Exception as e:
                e.args += (' op_id :' + str(self.op_id))
                raise

