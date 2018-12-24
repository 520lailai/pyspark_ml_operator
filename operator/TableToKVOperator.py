# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.sql import Row
from collections import OrderedDict

''' conf[]：
       selected_col_names:list[String] 
       append_col_names:list[String], 
    dataframe_list:  []
'''


class TableToKVOperator(Operator):

    def handle(self, dataframe_list, spark):
        selected_col_names = self.conf["selected_col_names"]
        append_col_names = self.conf["append_col_names"]
        df = dataframe_list[0]

        if not selected_col_names:
            selected_col_names = df.columns

        if df:
            dataframe = df.rdd.map(lambda row: map_function(row, selected_col_names, append_col_names)).toDF()
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError


def map_function(row, selected_col_names, append_col_names):
    new_row = {};
    kv_str = ""
    for col in append_col_names:
        new_row[col] = row[col]
    for col in selected_col_names:
        kv_str = kv_str + col + ":" + str(row[col]) + ","
    if len(kv_str) > 1:
        kv_str = kv_str[:len(kv_str) - 1]
    new_row["kv"] = kv_str
    return Row(**OrderedDict(new_row.items()))
