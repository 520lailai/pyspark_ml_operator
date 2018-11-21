# -*- coding: utf-8 -*-
from Operator import Operator
import redis

''' 
    conf[]：
       sql_query:String, 
    dataframe_list:  []
    spark : SparkSession
'''

class ApplyQuerySqlOperator(Operator):

    def handle(self, dataframe_list, spark):
        host = self.conf["host"]
        port = self.conf["port"]
        key = self.conf["key"]

        data = dataframe_list[0].collect()
        r = redis.StrictRedis(host = host ,port = port)
        r.set(key,data)


        if spark and sql:
            dataframe = spark.sql(sql)
            self.result_type = "single"
            self.status = "finished"
            return [dataframe]
        else:
            raise ValueError
