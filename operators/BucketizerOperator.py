# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.ml.feature import Bucketizer
from OperatorsUtils import *

''' 
    conf[]：
         splits: list[float],  ex:"-inf, 10, 50, inf"
         input_col:  String,   ex:"feature"
         output_col: String    ex:"bucketedfeature"
         is_drop_input:bool    ex: True  default=False
    dataframe_list:  []
'''


class BucketizerOperator(Operator):

    def handle(self, dataframe_list, spark):
        splits = self.conf["splits"]
        input_col = self.conf["input_col"]
        output_col = self.conf["output_col"]
        is_replace = self.conf["is_drop_input"]
        df = dataframe_list[0]

        check_dataframe(df)
        check_str_parameter(input_col, "the parameter:input_col is null!")
        check_str_parameter(output_col, "the parameter:output_col is null!")
        splits = str_convert_floatlist(splits)

        bucketizer = Bucketizer(splits=splits, inputCol=input_col, outputCol=output_col)
        bucketed_data = bucketizer.transform(df)
        if is_replace:
           bucketed_data.drop(input_col)
        return [bucketed_data]
