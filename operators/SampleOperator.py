# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

''' 
    conf[]：
         with_replacement: boolean,  ex:"False"
         fraction:         float,    ex:"0.98"  
         seed:             float,    ex:"243244"
    spark: SparkSession
'''


class SampleOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        with_replacement = self.conf["with_replacement"]
        fraction = float_convert(self.conf["fraction"])
        seed = self.conf["seed"]
        df = dataframe_list[0]

        if with_replacement is None:
            with_replacement = False
        else:
            with_replacement = bool_convert(with_replacement)

        if seed is None:
            seed = None
        else:
            seed = float_convert(seed)

        check_dataframe(df)
        dataframe = df.sample(with_replacement, fraction, seed)
        return [dataframe]
