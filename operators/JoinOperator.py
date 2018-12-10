# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

""" 
    模块功能： 实现两个表之间join
    
    用户必须指定join的列表达式： join_columns [[左表列名,右表列名]]
    用户可以指定左表的提取字段，以及别名： select_left_columns： [[左表的名，别名]]
    用户可以指定右表的提取字段，以及别名： select_right_columns：[[右表的名，别名]]
    用户可以指定 join 类型，目前的支持的类型：
       "inner", "cross",
       "outer","full","full_outer", 
       "left", "left_outer", "right",
       "right_outer", "left_semi","left_anti"

    conf 参数：
        "join_columns":         list[String]   ex: [["t1_col1", "t2_col1"],["t1_col2", "t2_col2"]]
        "select_left_columns":  list[String]   ex: [["t1_col1","col1_alia"],["t1_col2","col2_alia"]]
        "select_right_columns": list[String]   ex: [["t2_col1","col1_alia"],["t2_col2","col2_alia"]]
        "join_type" :           String         ex: 'inner'
    
    例子：
    
    1、输入的左表：
    +---+-------+-----+--------+
    |id1|country|hour1|clicked1|
    +---+-------+-----+--------+
    |  1|     US|   19|     1.0|
    |  2|     CA|    6|     4.0|
    |  3|     CA|   20|     0.0|
    |  4|     NO|    4|     7.0|
    +---+-------+-----+--------+
    2、输入右表：
    +---+-------+-----+--------+
    |id2|country|hour2|clicked2|
    +---+-------+-----+--------+
    |  1|     US|   19|     1.0|
    |  2|     CA|    6|     4.0|
    |  3|     CA|   20|     0.0|
    |  4|     NO|    4|     7.0|
    +---+-------+-----+--------+
        
    3、conf 参数：
    
    {
     'join_columns': [['id1', 'id2'], ['country', 'country']], 
     'select_right_columns': [['id2', 'id2'], ['country', 'country2'], ['hour2', 'hour2']], 
     'join_type': 'inner', 
     'select_left_columns': [['id1', 'id1'], ['country', 'country1'], ['hour1', 'hour1']]
     }
     
    4、结果表：
    +---+--------+-----+---+--------+-----+
    |id1|country1|hour1|id2|country2|hour2|
    +---+--------+-----+---+--------+-----+
    |  2|      CA|    6|  2|      CA|    6|
    |  3|      CA|   20|  3|      CA|   20|
    |  1|      US|   19|  1|      US|   19|
    |  4|      NO|    4|  4|      NO|    4|
    +---+--------+-----+---+--------+-----+
"""


class JoinOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数的获取
        join_columns = self.conf.get("join_columns")
        select_left_columns = self.conf.get("select_left_columns")
        select_right_columns = self.conf.get("select_right_columns")
        join_type = self.conf.get("join_type", "inner")

        # 1、参数的检查
        df1 = dataframe_list[0]
        df2 = dataframe_list[1]
        check_dataframe(df1)
        check_dataframe(df2)
        if not join_columns:
            raise ParameterException("the join_columns parameter is empty:" + str(join_columns))
        check_parameter_null_or_empty(join_type, "join_type")

        select_colums_list = []

        # 2、左表的提取字段
        left_columns_dict = {}
        if select_left_columns:
            for col in select_left_columns:
                left_columns_dict[col[0]] = col[1]
                df1 = df1.withColumnRenamed(col[0], col[1])
                select_colums_list.append(df1[col[1]])
        else:
            for name in df1.columns:
                left_columns_dict[name] = name
            select_colums_list.append(df1.columns)

        # 3、右表的提取字段
        right_columns_dict = {}
        if select_right_columns:
            for col in select_right_columns:
                right_columns_dict[col[0]] = col[1]
                df2 = df2.withColumnRenamed(col[0], col[1])
                select_colums_list.append(df2[col[1]])
        else:
            for name in df2.columns:
                right_columns_dict[name] = name
            select_colums_list.append(df2.columns)

        # 4、拼接join表达式
        express_list = []
        for two_colums in join_columns:
            express_list.append(df1[left_columns_dict[two_colums[0]]] == df2[right_columns_dict[two_colums[1]]])

        # 5、join操作
        dataframe = df1.join(df2, express_list, join_type).select(select_colums_list)
        return [dataframe]
