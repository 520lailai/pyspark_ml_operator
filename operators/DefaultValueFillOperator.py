# -*- coding: utf-8 -*-

from OperatorsUtils import *
from Operator import Operator

''' 
    conf[]: 
         col_name: list[String]  ex: 'age,name'
         col_value:list[String]  ex: '50,unknown'
    spark:  SparkSession
    dataframe_list:[df]
'''


class DefaultValueFillOperator(Operator):

    def handle(self, dataframe_list, spark):
        df = dataframe_list[0]
        col_name = self.conf["col_name"]
        col_value = self.conf["col_value"]

        check_dataframe(df)
        check_str_parameter(col_value, "the parameter:col_value is null")
        check_str_parameter(col_name, "the parameter:col_name is null")

        cols_list = col_name.split(",")
        values_str_list = col_value.split(",")
        col_value = convert_cols_parameter(df.schema.fields, cols_list, values_str_list)

        convert_cols_parameter(col_value, "the parameter:values is error")

        dataframe = df.na.fill(value=col_value, subset=col_name)
        return [dataframe]
