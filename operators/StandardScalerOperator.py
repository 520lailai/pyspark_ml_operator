# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.ml.feature import StandardScaler
from OperatorsUtils import *

''' 
    conf[]:
       "input_col": String, 
       "output_col": String, 
       "with_std": Boolean, True by default
       "with_mean": Boolean, False by default
    dataframe_list: []
'''


class StandardScalerOperator(Operator):

    def handle(self, dataframe_list, spark):
        input_col = self.conf["input_col"]
        output_col = self.conf["output_col"]
        with_std = self.conf["with_std"]
        with_mean = self.conf["with_mean"]
        df = dataframe_list[0]

        check_dataframe(df)
        check_str_parameter(input_col, "the parameter:input_col is null!")
        check_str_parameter(output_col, "the parameter:output_col is null!")

        if with_std is None:
            with_std = True
        else:
            with_std = bool_convert(with_std)

        if with_mean is None:
            with_mean = False
        else:
            with_mean = bool_convert(with_mean)

        scaler = StandardScaler(inputCol=input_col, outputCol=output_col,
                                withStd=with_std, withMean=with_mean)
        scaler_model = scaler.fit(df)
        scaled_data = scaler_model.transform(df)
        return [scaled_data]
