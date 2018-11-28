# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.sql.functions import when
from OperatorsUtils import *

''' 
    conf[]：
        smooth_conf:【input_col，min_thresh，max_thresh】
        [
            ["col1", "34,6","2452"]
            ["col2", "23","4500" ]
        ]
    dataframe_list: [df]
'''


class FeatureExceptionSmoothOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        smooth_conf = self.conf["smooth_conf"]
        df = dataframe_list[0]
        check_dataframe(df)

        for conf in smooth_conf:
            col_name = conf[0]
            min_thresh = float_convert(conf[1])
            max_thresh = float_convert(conf[2])

            check_str_parameter(col_name, "the parameter:col_name is null!")

            df = df.withColumn(col_name, when(df[col_name] > max_thresh, max_thresh).otherwise(df[col_name]))
            df = df.withColumn(col_name, when(df[col_name] < min_thresh, min_thresh).otherwise(df[col_name]))

        return [df]
