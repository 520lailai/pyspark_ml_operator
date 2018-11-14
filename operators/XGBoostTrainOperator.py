# -*- coding: utf-8 -*-
from Operator import Operator
import ml.dmlc.xgboost4j.scala.spark.{DataUtils, XGBoost}
import ml.dmlc.xgboost4j.scala.spark.{DataUtils, XGBoost}

''' 
    conf[]：
       "weights": list[], 
        "seed": flost, 
    dataframe_list: []
'''


class XGBoostTrainOperator(Operator):

    def handle(self, dataframe_list, spark):
        weights = self.conf["weights"]
        seed = self.conf["seed"]
        df = dataframe_list[0]

        if df:
            dataframe = df.randomSplit(weights, seed)
            self.result_type = "multi"
            self.status = "finished"
            redataframe = []
            for df in dataframe:
                redataframe.append(df)
            return redataframe
        else:
            raise ValueError
