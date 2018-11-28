# -*- coding: utf-8 -*-
from OperatorsUtils import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from DataProcessingOperator import DataProcessingOperator

''' conf[]：
       column_name: String,  ex:  "feature"
       label: String,  ex:  "id"
       output:String   ex: libsvm_feature
    dataframe_list:  []
'''


class LabelFeatureToLibsvm(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        column_name = self.conf["column_name"]
        label = self.conf["labels"]
        output = self.conf["output"]
        df = dataframe_list[0]
        check_dataframe(df)
        check_str_parameter(column_name, "the parameter:column_name is null!")
        check_str_parameter(label, "the parameter:filter_condition is null!")
        check_str_parameter(output, "the parameter:output is null!")
        rdd = df.rdd.map(lambda row: my_function(row))
        df = spark.createDataFrame(rdd, [output])
        return [df]


def my_function(row):
    pos = LabeledPoint(row["id"], row["clicked"])
    str = MLUtils._convert_labeled_point_to_libsvm(pos)
    return Row(str)
