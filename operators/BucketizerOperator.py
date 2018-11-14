# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.ml.feature import Bucketizer

''' 
    conf[]：
       "splits": [], 
        "input_col": String,
        "output_col": String 
    dataframe_list:  []
'''


class BucketizerOperator(Operator):

    def handle(self, dataframe_list, spark):
        splits = self.conf["splits"]
        input_col = self.conf["input_col"]
        output_col = self.conf["output_col"]
        df = dataframe_list[0]

        if df:
            bucketizer = Bucketizer(splits=splits, inputCol=input_col, outputCol=output_col)
            bucketed_data = bucketizer.transform(df)
            self.result_type = "single"
            self.status = "finished"
            return [bucketed_data]
        else:
            raise ValueError
