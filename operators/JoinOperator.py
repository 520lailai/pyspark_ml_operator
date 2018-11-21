# -*- coding: utf-8 -*-
from Operator import Operator

''' 
    conf[]：
        "using_columns": String, 
        "join_type": String 
    dataframe_list:  []
'''


class JoinOperator(Operator):
    # 参数：withReplacement:boolean, fraction:String，seed:String
    def handle(self, dataframe_list, spark):
        using_columns = self.conf["using_columns"]
        join_type = self.conf["join_type"]
        dataframe1 = dataframe_list[0]
        dataframe2 = dataframe_list[1]

        if dataframe1 and dataframe2:
            dataframe = dataframe1.join(dataframe2, using_columns, join_type)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise InputDataFrameException


class InputDataFrameException(BaseException):
    def __init__(self, mesg="input dataframe null!"):
        logging mesg
