# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.ml.feature import StandardScaler

''' 
    conf[]:
       "input_col": String, 
       "output_col": String, 
       "with_std": Boolean, 
       "with_mean": Boolean
    dataframe_list: []
'''


class StandardScalerOperator(Operator):

    def handle(self, dataframe_list, spark):
        input_col = self.conf["input_col"]
        output_col = self.conf["output_col"]
        with_std = self.conf["with_std"]
        with_mean = self.conf["with_mean"]
        df = dataframe_list[0]

        if df:
            scaler = StandardScaler(inputCol=input_col, outputCol=output_col,
                                    withStd=with_std, withMean=with_mean)
            scalerModel = scaler.fit(df)
            scaledData = scalerModel.transform(df)

            self.result_type = "single"
            self.status = "finished"
            return [scaledData]
        else:
            raise ValueError
