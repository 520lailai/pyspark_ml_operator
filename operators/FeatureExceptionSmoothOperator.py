# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.sql.functions import when

''' 
    conf[]：
        "input_col": "hour", 
        "min_thresh": "7", 
        "max_thresh": "15" 
    dataframe_list: []
'''


class FeatureExceptionSmoothOperator(Operator):
    def handle(self, dataframe_list, spark):
        col_name = self.conf["input_col"]
        min_thresh = self.conf["min_thresh"]
        max_thresh = self.conf["max_thresh"]
        df = dataframe_list[0]

        df1 = df.withColumn(col_name, when(df[col_name] > max_thresh, max_thresh).otherwise(df[col_name]))
        df2 = df1.withColumn(col_name, when(df1[col_name] < min_thresh, min_thresh).otherwise(df1[col_name]))

        self.result_type = "single"
        self.status = "finished"

        return [df2]


class partitionValException(BaseException):
    def __init__(self, mesg="this is a partition table, does not have partitionVal"):
        logging mesg
