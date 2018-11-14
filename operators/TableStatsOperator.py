# -*- coding: utf-8 -*-
from Operator import Operator

''' 
   conf[]：
       "cols":[], 
    dataframe_list:  []
'''


class TableStatsOperator(Operator):

    def handle(self, dataframe_list, spark):
        cols = self.conf["cols"]
        df = dataframe_list[0]
        if df:
            dataframe = df.describe(cols)
            self.result_type = "single"
            self.status = "finished"
            dataframe.show()
            return [dataframe]
        else:
            raise ValueError
