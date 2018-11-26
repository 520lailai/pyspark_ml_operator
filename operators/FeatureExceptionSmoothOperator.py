# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.sql.functions import when
import logging
from OperatorsUtils import *

''' 
    conf[]：
        input_col:  String  ex: "hour"
        min_thresh: float   ex: "7.0"
        max_thresh: float   ex: "15.0"
    dataframe_list: [df]
'''


class FeatureExceptionSmoothOperator(Operator):
    def handle(self, dataframe_list, spark):
        col_name = self.conf["input_col"]
        min_thresh = self.conf["min_thresh"]
        max_thresh = self.conf["max_thresh"]
        df = dataframe_list[0]

        min_thresh = float_convert(min_thresh)
        max_thresh = float_convert(max_thresh)

        check_dataframe(df)
        check_str_parameter(col_name, "the parameter:col_name is null!")

        df1 = df.withColumn(col_name, when(df[col_name] > max_thresh, max_thresh).otherwise(df[col_name]))
        df2 = df1.withColumn(col_name, when(df1[col_name] < min_thresh, min_thresh).otherwise(df1[col_name]))

        return [df2]
