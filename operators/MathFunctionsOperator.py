# -*- coding: utf-8 -*-
from Operator import Operator

''' 
    conf[]：
       "sql_expressions": set[String], 
    dataframe_list:  []
'''


class MathFunctionsOperator(Operator):

    def handle(self, dataframe_list, spark):
        function_expressions = self.conf["function_expressions"]
        dataset = dataframe_list[0]
        if dataset:
            dataframe = dataset.selectExpr(function_expressions)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
