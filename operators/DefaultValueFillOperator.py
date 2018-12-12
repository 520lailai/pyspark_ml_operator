# -*- coding: utf-8 -*-

from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *

''' 
    模块功能： 对于指定列中包含None的值进行指定值的填充
    列的类型只能是：int, long, float, string, bool
    
    conf 参数：
         "col_name_value" : 格式：[[列名, 填充值]]
    例子：
    
    1、输入表：
        +---+-------+----+-------+
        | id|country|hour|clicked|
        +---+-------+----+-------+
        |  1|     US|  18|    1.0|
        |  2|     CA|  12|   null|
        |  3|     CA|  12|    0.0|
        |  4|     CA|  14|    7.0|
        |  5|     SA|null|    0.0|
        |  6|     BA|  16|    3.0|
        |  7|     UA|  12|    0.0|
        |  8|   null|  18|   null|
        |  9|     PZ|  15|    0.0|
        +---+-------+----+-------+
    
    2、conf参数：
    
        {
           'col_name_value': 
           [
               ['country', 'hina'], 
               ['hour', '100'], 
               ['clicked', '99.99']
           ]
        }
       
    3、结果表：
        +---+-------+----+-------+
        | id|country|hour|clicked|
        +---+-------+----+-------+
        |  1|     US|  18|    1.0|
        |  2|     CA|  12|  99.99|
        |  3|     CA|  12|    0.0|
        |  4|     CA|  14|    7.0|
        |  5|     SA| 100|    0.0|
        |  6|     BA|  16|    3.0|
        |  7|     UA|  12|    0.0|
        |  8|  China|  18|  99.99|
        |  9|     PZ|  15|    0.0|
        +---+-------+----+-------+
'''


class DefaultValueFillOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        df = dataframe_list[0]
        col_name_value = self.conf.get("col_name_value")
        # 2、参数检查
        check_parameter_null_or_empty(col_name_value, "col_name_value", self.op_id)
        check_dataframe(df, self.op_id)
        # 3、参数格式化
        col_value_dict = convert_cols_parameter(df.schema.fields, col_name_value, self.op_id)
        # 4、默认值填充
        dataframe = df.na.fill(value=col_value_dict, subset=None)
        return [dataframe]
