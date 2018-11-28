# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

''' 
    conf[]：
        "left_weight":  float,   "0.2"
        "right_weight": float    "0.8"
        "seed": float, "2134"
    dataframe_list: []
'''


class RandomSplitOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        left_weight = float_convert(self.conf["left_weight"])
        right_weight = float_convert(self.conf["right_weight"])
        seed = self.conf["seed"]
        df = dataframe_list[0]

        check_dataframe(df)

        weights = [left_weight, right_weight]

        if seed is None:
            seed = None
        else:
            seed = float_convert(seed)

        dataframe = df.randomSplit(weights, seed)
        redataframe = []
        for df in dataframe:
            redataframe.append(df)
        return redataframe
