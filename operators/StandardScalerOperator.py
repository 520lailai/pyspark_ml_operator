# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.ml.feature import StandardScaler
from tools.OperatorsParameterParseUtils import *

""" 
    模块功能： 将特征中的值进行标准差标准化，即转换为均值为0，方差为1的正态分布
    conf[]:
       "standard_scaler_conf"：
        格式：[[input_col, output_col, with_std, with_mean,is_drop_input]]
            "input_col":  String, 输入的列名
            "output_col": String, 输出的列名
            "with_std":  bool, 默认为True是否将数据标准化到单位标准差
            "with_mean": bool, 默认为False 是否变换为0均值
            "is_drop_input": bool 是否删除原始列
        
    例子：
    
    1、输入表：
    +---+--------------+
    | id|      features|
    +---+--------------+
    |  0|[1.0,0.1,-1.0]|
    |  1| [2.0,1.1,1.0]|
    |  2|[3.0,10.1,3.0]|
    +---+--------------+
    
    2、conf配置参数：
    
    {'standard_scaler_conf': [['features', 'scaled_features', 'True', 'False', 'False']]}
    
    3、输出表：
    +---+--------------+-------------------------------+
    |id |features      |scaled_features                |
    +---+--------------+-------------------------------+
    |0  |[1.0,0.1,-1.0]|[1.0,0.018156825980064073,-0.5]|
    |1  |[2.0,1.1,1.0] |[2.0,0.19972508578070483,0.5]  |
    |2  |[3.0,10.1,3.0]|[3.0,1.8338394239864713,1.5]   |
    +---+--------------+-------------------------------+
"""


class StandardScalerOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        standard_scaler_conf = self.conf.get("standard_scaler_conf")
        df = dataframe_list[0]
        check_dataframe(df, self.op_id)
        check_parameter_null_or_empty(standard_scaler_conf, "standard_scaler_conf", self.op_id)

        for conf in standard_scaler_conf:
            if len(conf) < 4:
                raise ParameterException("the lengths of parameter must more than  4:" + str(conf))

            input_col = conf[0]
            check_cols([input_col], df.columns, self.op_id)

            output_col = conf[1]
            with_std = bool_convert(conf[2], self.op_id)
            with_mean = bool_convert(conf[3], self.op_id)
            is_drop_input = bool_convert(conf[4], self.op_id)

            check_parameter_null_or_empty(input_col, "input_col", self.op_id)
            check_parameter_null_or_empty(output_col, "output_col", self.op_id)

            if with_std is None:
                with_std = True
            else:
                with_std = bool_convert(with_std, self.op_id)

            if with_mean is None:
                with_mean = False
            else:
                with_mean = bool_convert(with_mean, self.op_id)

            scaler = StandardScaler(inputCol=input_col, outputCol=output_col,
                                    withStd=with_std, withMean=with_mean)
            scaler_model = scaler.fit(df)
            df = scaler_model.transform(df)

            if is_drop_input:
                df.drop(input_col)

        return [df]
