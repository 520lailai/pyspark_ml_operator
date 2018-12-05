# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *
from pyspark.sql.functions import col
from pyspark.ml.feature import MinMaxScaler

''' 
    conf[]：
        normalize_scaler_conf = List【input_col、output_col、min、max、is_drop_input 】
    dataframe_list:[]
'''


class NormalizedOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        normalize_scaler_conf = self.conf["normalize_scaler_conf"]
        df = dataframe_list[0]
        check_dataframe(df)
        if not normalize_scaler_conf:
            raise ParameterException("the parameter is empty : " + str(normalize_scaler_conf))

        type_dict = {}
        for tuple in df.dtypes:
            type_dict[tuple[0]] = tuple[1]

        for i, conf in enumerate(normalize_scaler_conf):
            if len(conf) < 4:
                raise ParameterException("the lengths of parameter must more than 5:" + str(conf))
            input_col = conf[0]
            output_col = conf[1]
            min = float_convert(conf[2])
            max = float_convert(conf[3])
            is_drop_input = bool_convert(conf[4])

            check_str_parameter(input_col, "the parameter:input_col is null!")
            check_str_parameter(output_col, "the parameter:output_col is null!")

            if not min:
                min = 0;
            if not max:
                max = 1

            type = type_dict[input_col]
            if type == "vector":
                df = min_max_scaler(df, input_col, output_col, min, max)
            elif type == 'bigint' or type == 'double':
                max_value = df.agg({input_col: "max"}).collect()[0][0]
                min_value = df.agg({input_col: "min"}).collect()[0][0]
                df = df.withColumn(output_col, normalized(col(input_col), max_value, min_value))
            else:
                raise ParameterException("input col must be bigint/double/vector,does not support: " + type)
            if is_drop_input:
                df = df.drop(input_col)
        return [df]


def normalized(value, max_value, min_value):
    if max_value == min_value:
        return value
    return (value - min_value) / (max_value - min_value)


def min_max_scaler(df, input_col, output_col, min, max):
    check_str_parameter(input_col, "the parameter:input_col is null!")
    check_str_parameter(output_col, "the parameter:output_col is null!")
    scaler = MinMaxScaler(inputCol=input_col, outputCol=output_col, min=min, max=max)
    scaler_model = scaler.fit(df)
    scaled_data = scaler_model.transform(df)
    return scaled_data
