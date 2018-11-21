# -*- coding: utf-8 -*-
from Operator import Operator
import redis

''' 
    conf[]：
       sql_query:String, 
    dataframe_list:  []
    spark : SparkSession
'''



class Database:
    def __init__(self):
        self.host = 'localhost'
        self.port = 6379


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

def write(self, website, city, year, month, day, deal_number):
    try:
        key = '_'.join([website,city,str(year),str(month),str(day)])
        val = deal_number
        r = redis.StrictRedis(host=self.host,port=self.port)
        r.set(key,val)
    except Exception, exception:
        print exception