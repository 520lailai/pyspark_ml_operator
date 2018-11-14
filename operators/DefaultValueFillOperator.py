# -*- coding: utf-8 -*-
from Operator import Operator

''' 
    conf[]: 
         value: int, long, float, string, or dict, 
             If the value is a dict, then `subset` is ignored 
             example: {'age': 50, 'name': 'unknown'})
         subset:[], optional list of column names to consider
    spark:  SparkSession
'''


class DefaultValueFillOperator(Operator):

    def handle(self, dataframe_list, spark):
        value = self.conf["value"]
        subset = self.conf["subset"]
        df = dataframe_list[0]

        if df:
            dataframe = df.na.fill(value=value, subset=subset)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
