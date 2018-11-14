# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.sql import functions

''' 
    conf[]：
       "input_col": String, 
       "function_name": String: log10 log2 abs max min sqrt
    dataframe_list:  []
'''


class MathFunctionsOperator(Operator):

    def handle(self, dataframe_list, spark):
        input_col = self.conf["input_col"]
        function_name = self.conf["function_name"]
        dataset = dataframe_list[0]

        if dataset:
            if function_name == "log10":
                dataframe = dataset.select(functions.log10(dataset[input_col]))
            if function_name == "log2":
                dataframe = dataset.select(functions.log2(dataset[input_col]))
            if function_name == "abs":
                dataframe = dataset.select(functions.abs(dataset[input_col]))
            if function_name == "max":
                dataframe = dataset.select(functions.max(dataset[input_col]))
            if function_name == "min":
                dataframe = dataset.select(functions.min(dataset[input_col]))
            if function_name == "sqrt":
                dataframe = dataset.select(functions.sqrt(dataset[input_col]))

            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
