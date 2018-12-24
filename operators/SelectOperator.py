# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from backend.framework.tools.OperatorsParameterParseUtils import *

""" 
    模块功能： 过滤查询，用户通过指定相关的列和一些刷选条件进行数据的查询
    conf 参数：
        "column_names_alias":List[String,string], 提取的列名列表 ex:  ["id1", "id2"], ["clicked1", "clicked1"]
        "filter_condition" : String,  where表达式              ex:   "hour1>=10 and country == 'US'"
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
    
    {'filter_condition': 'hour>=15', 'column_names_alias': [['country', 'country2'],['clicked', 'clicked2']]}
    
    3、结果表：
    +-------+----------+
    |country2|clicked2 |
    +-------+----------+
    |US     |1.0       |
    |NZ     |0.0       |
    +-------+----------+
"""


class SelectOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        column_names_alias = self.conf.get("column_names_alias")
        filter_condition = self.conf.get("filter_condition")
        check_parameter_null_or_empty(dataframe_list, "dataframe_list", self.op_id)
        dataframe = dataframe_list[0]

        col_names = []
        alia_names = []

        # 2、参数检测
        check_parameter_null_or_empty(column_names_alias,"column_names_alias", self.op_id)
        for col_alias in column_names_alias:
            if not col_alias or len(col_alias) != 2:
                raise ParameterException("[arthur_error] the input parameter:col_alias must be a n*2 matric")
            col_names.append(col_alias[0])
            alia_names.append(col_alias[1])

        check_dataframe(dataframe, self.op_id)
        check_strlist_parameter(col_names, "col_names", self.op_id)
        try:
            # 3、过滤查询
            if filter_condition:
                dataframe = dataframe.select(col_names).filter(filter_condition)
            else:
                dataframe = dataframe.select(col_names)

            for i, alia in enumerate(alia_names):
                if alia:
                    dataframe = dataframe.withColumnRenamed(col_names[i], alia)
            return [dataframe]
        except Exception as e:
            e.args += (' op_id :' + str(self.op_id),)
            raise
