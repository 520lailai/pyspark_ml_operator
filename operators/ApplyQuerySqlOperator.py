# -*- coding: utf-8 -*-
from Operator import Operator

''' 
    conf[]：
       sql_query:String, 
    dataframe_list:  []
    spark : SparkSession
'''


class ApplyQuerySqlOperator(Operator):

    def handle(self, dataframe_list, spark):
        sql = self.conf["sql_query"]
        if spark and sql:
            dataframe = spark.sql(sql)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
