# -*- coding: utf-8 -*-
from Operator import Operator

''' 
    conf[]：
         withReplacement: boolean, can elements be sampled multiple times 
         fraction: float, size of the sample as a fraction of this RDD's size
         seed:float, seed for the random number generator
    spark: SparkSession
'''


class SampleOperator(Operator):

    def handle(self, dataframe_list, spark):
        with_replacement = self.conf["with_replacement"]
        fraction = self.conf["fraction"]
        seed = self.conf["seed"]
        df = dataframe_list[0]

        if df:
            dataframe = df.sample(with_replacement, fraction, seed)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
