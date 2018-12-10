# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

""" 
    模块功能：计算DataFrame的数值列的近似分位数。
    用户只要选定某些列(input_cols),
    分位数列表(probabilities),
    指定相对误差(relative_error),relative_error 取的是0到1之前的小数，值越小，准确性越高。
    就可以返回对应的的分位值列表
    
    conf 参数：
        "input_cols":     list [String]      ex:"hour,clicked"   
        "probabilities":  list [float]       ex:"0.5,0.7,0.8,0.9", 
        "relative_error": float              ex:"0.1"   default:1
    
    例子：
       1. 输入的表：
        +---+-------+----+-------+
        | id|country|hour|clicked|
        +---+-------+----+-------+
        |  1|     US|  18|    1.0|
        |  2|     CA|  12|    0.0|
        |  3|     NZ|  15|    0.0|
        +---+-------+----+-------+
        
       2. conf参数:
         { 
           'probabilities'  : '0.5, 0.75, 0.9, 0.95, 0.99', 
           'input_cols'     : 'hour, clicked', 
           'relative_error' : '0.8'
         }
         
       3. 结果表：
        +----------+----+-----+----+-----+-----+
        |colum_name|p0.5|p0.75|p0.9|p0.95|p0.99|
        +----------+----+-----+----+-----+-----+
        |      hour|12.0| 12.0|18.0| 18.0| 18.0|
        |   clicked| 0.0|  0.0| 1.0|  1.0|  1.0|
        +----------+----+-----+----+-----+-----+
"""


class ApproxQuantileOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        input_cols = self.conf.get("input_cols")
        probabilitie_str = self.conf.get("probabilities", "0.50,0.60,0.70,0.80,0.90")
        relative_error = self.conf.get("relative_error", "1")
        df = dataframe_list[0]

        # 2、参数转换与检查
        input_cols = str_convert_strlist(input_cols)
        probabilities = str_convert_floatlist(probabilitie_str)
        relative_error = float_convert(relative_error)
        check_dataframe(df)

        # 3、分位计算
        quantile_list = df.approxQuantile(input_cols, probabilities, relative_error)

        # 4、构建格式输出表
        for i, quantile in enumerate(quantile_list):
            quantile.insert(0, input_cols[i])

        schema = ["colum_name"]
        for p in probabilities:
            schema.append("p" + str(p))

        dataset = spark.createDataFrame(quantile_list, schema)
        return [dataset]
