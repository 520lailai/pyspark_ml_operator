# -*- coding: utf-8 -*-
from Operator import Operator
from OperatorsUtils import *

''' 
    conf[]：
       "col_names": list[String], ex: 'hour,clicked' 
       "scale_method" :           ex: 'log10,sqrt'
       "is_replace":              ex: 'True, False'
       "new_col_name" :           ex: 'hour, scaled_clicked'
    dataframe_list:  []
'''


class MathFunctionsOperator(Operator):

    def handle(self, dataframe_list, spark):
        col_names = str_convert_strlist(self.conf["col_names"])
        scale_method = str_convert_strlist(self.conf["scale_method"])
        is_replace = str_convert_strlist(self.conf["is_replace"])
        new_col_name = str_convert_strlist(self.conf["new_col_name"])
        df = dataframe_list[0]

        # parameter check,
        check_dataframe(df)
        check_str_parameter(col_names)
        check_str_parameter(scale_method)
        check_str_parameter(is_replace)
        check_str_parameter(new_col_name)
        if not (len(col_names) == len(scale_method) ==len(is_replace) == len(new_col_name)) :
            raise ParameterException("the Parameter error")

        cols = df.columns
        check_cols(col_names, cols)

        replace_index = []
        for col in col_names:
            index = col_names.index(col)
            method_express = scale_method[index] + "(" + col + ")"
            if is_replace[index]:
                cols[cols.index(col)] = method_express
            else:
                cols.append(method_express)
            replace_index.append(cols.index(method_express))

        dataframe = df.selectExpr(cols)
        new_colums = dataframe.columns
        for i, new_name in enumerate(new_col_name):
            dataframe = dataframe.withColumnRenamed(new_colums[replace_index[i]],new_name)
        return [dataframe]
