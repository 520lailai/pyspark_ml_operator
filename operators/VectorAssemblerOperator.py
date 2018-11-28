# -*- coding: utf-8 -*-
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from OperatorsUtils import *
from DataProcessingOperator import  DataProcessingOperator

''' conf[]：
       column_names: List[String],  ex:  ["id1","hour1","clicked1"]
       output:String
    dataframe_list:  []
'''


class VectorAssemblerOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        column_names = self.conf["column_names"]
        output = self.conf["output"]
        df = dataframe_list[0]
        check_dataframe(df)
        check_strlist_parameter(column_names)
        check_str_parameter(output, "the parameter:output is null!")

        assembler = VectorAssembler(inputCols=column_names, outputCol=output)
        df = assembler.transform(df)
        return [df]
