# -*- coding: utf-8 -*-

from OperatorsUtils import *
from DataProcessingOperator import DataProcessingOperator

''' 
    conf[]: 
         col_name_value: list[[String]]  
         [
            ["col1", "34"],
            ["col2", 'hah'],
            ["col3","89.9"]
         ]
    spark:  SparkSession
    dataframe_list:[df]
'''


class DefaultValueFillOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        df = dataframe_list[0]
        col_name_value = self.conf["col_name_value"]
        check_dataframe(df)
        col_value_dict = convert_cols_parameter(df.schema.fields, col_name_value)
        dataframe = df.na.fill(value=col_value_dict, subset=None)
        return [dataframe]
