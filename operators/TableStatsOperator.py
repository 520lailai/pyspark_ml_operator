# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *
from WriteRedisOperator import WriteRedisOperator

''' 
   conf[]：
       "cols":[],
   dataframe_list: []
'''


class TableStatsOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        cols = self.conf["cols"]
        df = dataframe_list[0]
        check_dataframe(df)
        if cols is None:
            dataframe = df.summary()
        else:
            check_strlist_parameter(cols)
            dataframe = df.select(cols).summary()
        return [dataframe]
