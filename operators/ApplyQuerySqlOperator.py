# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *

""" 
    模块功能： 运行用户自定义查询sql
    
    conf 参数：
         "sql_query": String 用户查询的语句
"""


class ApplyQuerySqlOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1. 参数解析
        sql_query = self.conf.get("sql_query")

        # 2. 参数检查
        check_parameter_null_or_empty(sql_query, "sql_query")
        check_parameter_null_or_empty(spark, "spark")

        # 3. 运行sql
        dataframe = spark.sql(sql_query)

        return [dataframe]
