# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

''' 
    conf[]：
       "scaler_conf" :  [ 
           [ "col1", "log2", "True", "scaler_col1"],
           [ "col2", "ln", "True", "scaler_col2"],
           [ "col3", "sqrt", "True", "scaler_col3"]
        ] 
    dataframe_list:  []
'''


class MathFunctionsOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        df = dataframe_list[0]
        check_dataframe(df)
        scaler_conf = self.conf["scaler_conf"]
        if not scaler_conf:
            raise ParameterException("the parameter is null")

        col_names = []
        scale_method = []
        is_replace = []
        new_col_name = []

        for conf in scaler_conf:
            col_names.append(conf[0])
            scale_method.append(conf[1])
            is_replace.append(bool_convert(conf[2]))
            new_col_name.append(conf[3])
            # parameter check,
            check_str_parameter(col_names, "the Parameter:col_names is null")
            check_str_parameter(scale_method, "the Parameter:scale_method is null")
            check_str_parameter(is_replace, "the Parameter:col_names is null")
            check_str_parameter(new_col_name, "the Parameter:new_col_name is null")
            if not (len(col_names) == len(scale_method) == len(is_replace) == len(new_col_name)):
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
            dataframe = dataframe.withColumnRenamed(new_colums[replace_index[i]], new_name)
        return [dataframe]
