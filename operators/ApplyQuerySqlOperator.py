# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from backend.framework.tools.OperatorsParameterParseUtils import *
import traceback

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
        check_parameter_null_or_empty(sql_query, "sql_query", self.op_id)
        check_parameter_null_or_empty(spark, "spark", self.op_id)

        # 3. 运行sql
        try:
            dataframe = spark.sql(sql_query)
            return [dataframe]
        except Exception as e:
            e.args += (' op_id :' + str(self.op_id),)
            raise
