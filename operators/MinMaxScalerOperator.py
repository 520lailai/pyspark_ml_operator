# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.ml.feature import MinMaxScaler
from OperatorsUtils import *

''' 
    conf[]：
        input_col  :  String 
        output_col :  String
        min : float
        max : float     
    dataframe_list:[]
'''


class MinMaxScalerOperator(Operator):
    def handle(self, dataframe_list, spark):
        input_col = self.conf["input_col"]
        output_col = self.conf["output_col"]
        min = float_convert(self.conf["min"])
        max = float_convert(self.conf["max"])
        df = dataframe_list[0]

        check_dataframe(df)
        check_str_parameter(input_col, "the parameter:input_col is null!")
        check_str_parameter(output_col, "the parameter:output_col is null!")

        scaler = MinMaxScaler(inputCol=input_col, outputCol=output_col, min=min, max=max)
        scaler_model = scaler.fit(df)
        scaled_data = scaler_model.transform(df)
        return [scaled_data]
