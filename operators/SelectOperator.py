# -*- coding: utf-8 -*-
from Operator import Operator

''' conf[]：
       sql:String, 
    dataframe_list:  []
'''


class SelectOperator(Operator):

    def handle(self, dataframe_list, spark):
        columnName = self.conf["columnName"]
        filterCondition = self.conf["filterCondition"]
        df = dataframe_list[0]

        if df:
            dataframe = df.select(columnName).filter(filterCondition)
            self.result_type = "single"
            self.status = "finished"
            dataframe.show()
            return [dataframe]
        else:
            raise ValueError
