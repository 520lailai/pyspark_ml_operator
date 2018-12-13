# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.sql.functions import when
from tools.OperatorsParameterParseUtils import *

""" 
    模块功能： 用户指定数值类型的某些列，将该列中分布在[minThresh, maxThresh]之外的数据
    平滑到minThresh和maxThresh这两个数据点之内。
    
    conf 参数：
        "smooth_conf": 格式：[[输入列名，最小阈值，最大阈值]]
        
    例子：
    
    1、输入表：
        +---+-------+----+-------+
        | id|country|hour|clicked|
        +---+-------+----+-------+
        |  1|     US|  18|    1.0|
        |  2|     CA|  12|   10.0|
        |  3|     CA|   5|  100.0|
        |  4|     CA|   4|   17.0|
        |  5|     NZ|  15|    8.0|
        +---+-------+----+-------+
    
    2、conf配置:
    
        {'smooth_conf': [['hour', '7', '15'], ['clicked', '10', '50']]}
        
    3、输出表：
        +---+-------+----+-------+
        | id|country|hour|clicked|
        +---+-------+----+-------+
        |  1|     US|15.0|   10.0|
        |  2|     CA|12.0|   10.0|
        |  3|     CA| 7.0|   50.0|
        |  4|     CA| 7.0|   17.0|
        |  5|     NZ|15.0|   10.0|
        +---+-------+----+-------+  
"""


class FeatureExceptionSmoothOperator(DataProcessingOperator):
    support_type = ["bigint", "smallint", "int", "tinyint", "double", "float", "numeric"]

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        smooth_conf = self.conf.get("smooth_conf")

        # 2、参数检测
        check_parameter_null_or_empty(smooth_conf, "smooth_conf", self.op_id)
        df = dataframe_list[0]
        check_dataframe(df, self.op_id)
        df_schema = get_df_schema(df)
        try:
            # 3、特征平滑
            for conf in smooth_conf:
                col_name = conf[0]
                check_cols([col_name], df.columns, self.op_id)
                col_type = df_schema.get(col_name)
                if col_type not in self.support_type:
                    raise ParameterException("[arthur_error] the input colums type must be a number type, but now is a :"+str(col_type))

                min_thresh = float_convert(conf[1], self.op_id)
                max_thresh = float_convert(conf[2], self.op_id)

                if "int" in col_type:
                    min_thresh = int_convert(conf[1], self.op_id)
                    max_thresh = int_convert(conf[2], self.op_id)

                df = df.withColumn(col_name, when(df[col_name] > max_thresh, max_thresh).otherwise(df[col_name]))
                df = df.withColumn(col_name, when(df[col_name] < min_thresh, min_thresh).otherwise(df[col_name]))
            return [df]
        except Exception as e:
            e.args += ' op_id :'+ str(self.op_id)
            raise
