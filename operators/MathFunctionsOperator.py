# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *

""" 
    模块功能： 特征的尺度变换
    对某一列进行 log2、log、ln、abs、sqrt 等函数计算
    conf 参数：
        "scaler_conf" : 格式：[[ "列名", "计算函数", "是否替换原列", "新列名"]]
    例子：
    
    1、输入的表：
    +---+-------+----+-------+
    | id|country|hour|clicked|
    +---+-------+----+-------+
    |  1|     US|  18|    1.0|
    |  2|     CA|  12|    0.0|
    |  3|     NZ|  15|    0.0|
    +---+-------+----+-------+
    
    2、conf参数：
    {
    'scaler_conf': 
      [
        ['hour', 'log2', 'True', 'scaled_hour'], 
        ['clicked', 'sqrt', 'False', 'scaled_clicked']
      ]
    }
    
    3、输入的表
    +---+-------+------------------+-------+--------------+
    | id|country|       scaled_hour|clicked|scaled_clicked|
    +---+-------+------------------+-------+--------------+
    |  1|     US| 4.169925001442312|    1.0|           1.0|
    |  2|     CA|3.5849625007211565|    0.0|           0.0|
    |  3|     NZ|3.9068905956085187|    0.0|           0.0|
    +---+-------+------------------+-------+--------------+
"""


class MathFunctionsOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数的获取和解析
        df = dataframe_list[0]
        check_dataframe(df, self.op_id)

        scaler_conf = self.conf.get("scaler_conf")
        check_parameter_null_or_empty(scaler_conf, "scaler_conf", self.op_id)

        col_names = []
        scale_method = []
        is_replace = []
        new_col_name = []

        for conf in scaler_conf:
            col_names.append(conf[0])
            scale_method.append(conf[1])
            is_replace.append(bool_convert(conf[2], self.op_id))
            new_col_name.append(conf[3])

        # 2、参数的检查
        check_strlist_parameter(col_names, self.op_id)
        check_strlist_parameter(scale_method, self.op_id)
        check_strlist_parameter(new_col_name, self.op_id)

        if not (len(col_names) == len(scale_method) == len(is_replace) == len(new_col_name)):
            raise ParameterException("the Parameter error, opid:"+str(self.op_id))
        cols = df.columns
        check_cols(col_names, cols, self.op_id)


        # 3、计算函数的表达式
        replace_index = []
        for index, col in enumerate(col_names):
            method_express = scale_method[index] + "(" + col + ")"
            if is_replace[index]:
                cols[cols.index(col)] = method_express
            else:
                cols.append(method_express)
            replace_index.append(cols.index(method_express))

        try:
            # 4、列的计算
            dataframe = df.selectExpr(cols)

            # 5、新列名的替换
            new_colums = dataframe.columns
            for i, new_name in enumerate(new_col_name):
                dataframe = dataframe.withColumnRenamed(new_colums[replace_index[i]], new_name)
            return [dataframe]

        except Exception as e:
            e.args += (' op_id :'+ str(self.op_id),)
            raise