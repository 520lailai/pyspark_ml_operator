# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.sql import Row
from collections import OrderedDict
from OperatorsUtils import *

''' conf[]：
       selected_col_names: list[String] 
       append_col_names:   list[String], 
    dataframe_list:  []
'''


class TableToKVOperator(Operator):

    def handle(self, dataframe_list, spark):
        selected_col_names = self.conf["selected_col_names"]
        append_col_names = self.conf["append_col_names"]
        df = dataframe_list[0]

        check_dataframe(df)

        if not selected_col_names:
            selected_col_names = df.columns
        else:
            selected_col_names = str_convert_strlist(selected_col_names)

        if append_col_names is not None:
            append_col_names = str_convert_strlist(append_col_names)

        dataframe = df.rdd.map(lambda row: map_function(row, selected_col_names, append_col_names)).toDF()
        return [dataframe]


def map_function(row, selected_col_names, append_col_names):
    new_row = {};
    kv_str = ""
    for col in append_col_names:
        new_row[col] = row[col]
    for col in selected_col_names:
        if col and str(row[col]):
            kv_str = kv_str + col + ":" + str(row[col]) + ","
    if len(kv_str) > 1:
        kv_str = kv_str[:len(kv_str) - 1]
    new_row["kv"] = kv_str
    return Row(**OrderedDict(new_row.items()))
