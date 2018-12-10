# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *
from WriteRedisOperator import WriteRedisOperator

"""  
    模块功能： 全表的简单统计, 指定某些列，展示这些列的一些列的5个统计信息。 
    用于整体数值特征：最大值，最小值，均值，标准差,统计数，分位：p25，p50,p75
    conf 参数：
         "cols": String 列名列表，如果输入为None， 将会统计所有的数值型的列。
         
    例子：
    
    1、输入表：
    +---+-------+----+-------+
    | id|country|hour|clicked|
    +---+-------+----+-------+
    |  1|     US|  18|   10.0|
    |  2|     CA|  12|   20.0|
    |  3|     NZ|  15|   30.0|
    |  4|     HK|  19|   30.0|
    |  5|     MZ|  21|   30.0|
    +---+-------+----+-------+
    
    2、cof参数配置：
    
    'cols': ['hour', 'clicked']}
    
    3、结果表：
    
    +-------+------------------+----------------+
    |summary|hour              |clicked         |
    +-------+------------------+----------------+
    |count  |5                 |5               |
    |mean   |17.0              |24.0            |
    |stddev |3.5355339059327378|8.94427190999916|
    |min    |12                |10.0            |
    |25%    |15                |20.0            |
    |50%    |18                |30.0            |
    |75%    |19                |30.0            |
    |max    |21                |30.0            |
    +-------+------------------+----------------+
"""


class TableStatsOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        cols = self.conf.get("cols")
        df = dataframe_list[0]

        # 2、参数检查
        check_dataframe(df)
        check_parameter_null_or_empty(cols, "cols")

        # 3、全表统计
        if cols is None:
            dataframe = df.summary()
        else:
            check_strlist_parameter(cols)
            dataframe = df.select(cols).summary()
        return [dataframe]
