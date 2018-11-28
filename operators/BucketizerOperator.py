# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.ml.feature import Bucketizer
from OperatorsUtils import *

''' 
    conf[]：
         [   
              [ "equal_distance", "8", "col3", "col3_out","True"], 
              [ "custom_bucketizer", "-inf, 0, 2, inf", "col1", "col1_out", "True"],
              [ "custom_bucketizer", "-inf, 1, 2, inf", "col2", "col2_out", "True"]
         ] 
         
    dataframe_list:  []
'''


class BucketizerOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        bucketizer_conf = self.conf["bucketizer_conf"]
        df = dataframe_list[0]
        check_dataframe(df)

        for conf in bucketizer_conf:
            splits_type = conf[0]
            split = str_convert_floatlist(conf[1])
            input_col = conf[2]
            output_col = conf[3]
            is_drop_input = bool_convert(conf[4])

            if splits_type == "equal_distance":
                distance = split[0]
                max_value = df.agg({input_col: "max"}).collect()[0][0]
                min_value = df.agg({input_col: "min"}).collect()[0][0]
                split = getBucketSplits(max_value, min_value, distance)

            check_str_parameter(input_col, "the parameter:input_col is null!")
            check_str_parameter(output_col, "the parameter:output_col is null!")
            bucketizer = Bucketizer(splits=split, inputCol=input_col, outputCol=output_col)
            bucketed_data = bucketizer.transform(df)
            if is_drop_input:
                bucketed_data.drop(input_col)
        return [bucketed_data]


def getBucketSplits(max_value, min_value, distance):
    splits = []
    temp = min_value
    while temp < max_value:
        splits.append(temp)
        temp += distance
    return splits
