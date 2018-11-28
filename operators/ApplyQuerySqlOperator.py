# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

''' 
    conf[]：
       sql_query: String,  ex:"select * from lai_test.test1" 
    dataframe_list:[df]
    spark: SparkSession
'''


class ApplyQuerySqlOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        sql_query = self.conf["sql_query"]

        check_str_parameter(sql_query, "the parameter: sql_query is null!")
        check_str_parameter(spark, "the spark is null")

        dataframe = spark.sql(sql_query)
        return [dataframe]
