# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.sql.types import FloatType

''' 
   conf[]
        "input_col": "hour", 
        "probabilities": list [float], 
        "relative_error": float
    dataframe_list:[]
'''


class ApproxQuantileOperator(Operator):

    def handle(self, dataframe_list, spark):
        input_col = self.conf["input_col"]
        probabilities = self.conf["probabilities"]
        relative_error = self.conf["relative_error"]
        df = dataframe_list[0]
        if df:
            quantile_list = df.approxQuantile(input_col, probabilities, relative_error)
            self.result_type = "single"
            self.status = "finished"
            dataset = spark.createDataFrame(quantile_list, FloatType())
            return [dataset]
        else:
            raise ValueError
