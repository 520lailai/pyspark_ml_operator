# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.ml.feature import MinMaxScaler

''' 
    conf[]：
        input_col  : String,
        output_col :  String
        min : float
        max : flaot     
    dataframe_list:[]
'''


class MinMaxScalerOperator(Operator):
    def handle(self, dataframe_list, spark):
        input_col = self.conf["input_col"]
        output_col = self.conf["output_col"]
        min = self.conf["min"]
        max = self.conf["max"]
        df = dataframe_list[0]

        if df and input_col and output_col:
            scaler = MinMaxScaler(inputCol=input_col, outputCol=output_col, min=min, max=max)
            scaler_model = scaler.fit(df)
            scaled_data = scaler_model.transform(df)
            self.result_type = "single"
            self.status = "finished"
            return [scaled_data]
        else:
            raise ValueError
