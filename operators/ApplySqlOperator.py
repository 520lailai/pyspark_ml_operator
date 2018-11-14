# -*- coding: utf-8 -*-
from Operator import Operator

''' 
    conf[]：
       sql:String, 
    dataframe_list:  []
    spark : SparkSession
'''


class ApplySqlOperator(Operator):

    def handle(self, dataframe_list, spark):
        sql = self.conf["sql"]
        if spark and sql:
            dataframe = spark.sql(sql)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
