# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

""" 
    模块功能： 过滤查询，用户通过指定相关的列和一些刷选条件进行数据的查询
    conf 参数：
        "column_names":      List[String], 提取的列名列表    ex:  "id1,hour1,clicked1"
        "filter_condition" : String,  where表达式           ex: "hour1>=10 and country == 'US'"
    例子：
    
    1、原始表：
    
    +---+-------+----+-------+
    | id|country|hour|clicked|
    +---+-------+----+-------+
    |  1|     US|  18|    1.0|
    |  2|     CA|  12|    0.0|
    |  3|     NZ|  15|    0.0|
    +---+-------+----+-------+
    
    2、conf配置参数：
    
    {'filter_condition': 'hour>=15', 'column_names': ['country', 'clicked']}
    
    3、结果表：
    +-------+-------+
    |country|clicked|
    +-------+-------+
    |US     |1.0    |
    |NZ     |0.0    |
    +-------+-------+
"""


class SelectOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        column_names = self.conf.get("column_names")
        filter_condition = self.conf.get("filter_condition")
        df = dataframe_list[0]
        # 2、参数检测
        check_dataframe(df)
        check_strlist_parameter(column_names)
        column_names = str_convert_strlist(column_names)

        # 3、过滤查询
        if filter_condition:
            dataframe = df.select(column_names).filter(filter_condition)
        else:
            dataframe = df.select(column_names)
        return [dataframe]
