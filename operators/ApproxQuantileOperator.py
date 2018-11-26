# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.sql.types import FloatType
from OperatorsUtils import *
from WriteRedisOperator import WriteRedisOperator

''' 
   conf[]
        input_col:  list[String]      ex:"hour,clicked"   
        probabilities: list [float]   ex:"0.5,0.7,0.8,0.9", 
        relative_error: float         ex:"0.1"   default:1
   dataframe_list:[df]
'''


class ApproxQuantileOperator(Operator):

    def handle(self, dataframe_list, spark):
        # get string parameter
        input_col = self.conf["input_col"]
        probabilities = self.conf["probabilities"]
        relative_error = self.conf["relative_error"]
        df = dataframe_list[0]

        # check and convert input_col
        input_col = str_convert_strlist(input_col)

        # check and convert probabilities
        probabilities = str_convert_floatlist(probabilities)

        # check and convert relative_error
        if relative_error:
            relative_error = float_convert(relative_error)
        else:
            relative_error = 1

        # check df
        check_dataframe(df)

        quantile_list = df.approxQuantile(input_col, probabilities, relative_error)
        schema =[]
        for p in probabilities:
            schema.append("p"+str(p))

        dataset = spark.createDataFrame(quantile_list, schema)
        return [dataset]
