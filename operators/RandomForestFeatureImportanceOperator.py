# -*- coding: utf-8 -*-
import numpy
from Operator import Operator
from numpy import allclose
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import RandomForestClassifier

''' 
    conf[]：
         withReplacement: boolean, can elements be sampled multiple times 
         fraction: float, size of the sample as a fraction of this RDD's size
         seed:float, seed for the random number generator
    spark: SparkSession
'''


class RandomForestFeatureImportanceOperator(Operator):

    def handle(self, dataframe_list, spark):
        with_replacement = self.conf["with_replacement"]
        fraction = self.conf["fraction"]
        seed = self.conf["seed"]
        df = dataframe_list[0]

        if df:
            stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
            si_model = stringIndexer.fit(df)
            td = si_model.transform(df)
            rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="indexed", seed=42)
            model = rf.fit(td)
            model.featureImportances
            dataframe = df.sample(with_replacement, fraction, seed)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
