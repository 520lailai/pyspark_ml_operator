# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.ml.feature import OneHotEncoderEstimator
from OperatorsUtils import *

''' 
    conf[]：
        input_cols: [String]    ex: "hour, clicked"
        output_cols: [String],  ex: "onehot_hour, one_hot_clicked"
        drop_last: bool,        ex: "False"
        handle_invalid: String, ex:  None
    dataframe_list: []
'''


class OneHotEncoderEstimatorOperator(Operator):
    def handle(self, dataframe_list, spark):
        input_cols = str_convert_strlist(self.conf["input_cols"])
        output_cols = str_convert_strlist(self.conf["output_cols"])
        drop_last = self.conf["drop_last"]
        handle_invalid = self.conf["handle_invalid"]

        df = dataframe_list[0]

        check_dataframe(df)
        check_str_parameter(input_cols, "the parameter:input_cols is null!")
        check_str_parameter(output_cols, "the parameter:output_cols is null!")

        encoder = OneHotEncoderEstimator(inputCols=input_cols, outputCols=output_cols)
        if drop_last is not None:
            drop_last = bool_convert(drop_last)
            encoder.setDropLast(drop_last)

        if handle_invalid:
            encoder.setHandleInvalid(handle_invalid)

        model = encoder.fit(df)
        encoded = model.transform(df)
        return [encoded]
