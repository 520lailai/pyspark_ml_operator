# -*- coding: utf-8 -*-
from Operator import Operator

''' conf[]：
       column_name:String, 
       filter_condition : String, where expression
    dataframe_list:  []
'''


class SelectOperator(Operator):

    def handle(self, dataframe_list, spark):
        column_name = self.conf["column_name"]
        filter_condition = self.conf["filter_condition"]
        df = dataframe_list[0]

        if df:
            dataframe = df.select(column_name).filter(filter_condition)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
