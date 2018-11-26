# -*- coding: utf-8 -*-
from Operator import Operator
from OperatorsUtils import *
from WriteRedisOperator import WriteRedisOperator

''' 
   conf[]：
       "cols":[],
   dataframe_list: []
'''


class TableStatsOperator(Operator):

    def handle(self, dataframe_list, spark):
        cols = self.conf["cols"]
        df = dataframe_list[0]

        check_dataframe(df)
        dataframe = None

        if cols is None:
            dataframe = df.summary()
        else:
            check_strlist_parameter(cols)
            dataframe = df.select(cols).summary()

        return [dataframe]
