# -*- coding: utf-8 -*-
from Operator import Operator
from OperatorsUtils import *

''' 
    conf[]：
        "weights": list[],   "0.2,0.8"
        "seed": float,       "2134"
    dataframe_list: []
'''


class RandomSplitOperator(Operator):

    def handle(self, dataframe_list, spark):
        weights = self.conf["weights"]
        seed = self.conf["seed"]
        df = dataframe_list[0]

        check_dataframe(df)

        weights = str_convert_floatlist(weights)

        if seed is None:
            seed = None
        else:
            seed = float_convert(seed)

        dataframe = df.randomSplit(weights, seed)
        redataframe = []
        for df in dataframe:
            redataframe.append(df)
        return redataframe
