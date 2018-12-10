# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

''' 
    模块功能： 用户指定左右权重和随机数种子，把一个dataframe随机拆分成两份。
    
    conf 参数：
        "left_weight" :  float  左边权重
        "right_weight":  float  右边权重
        "seed": float 随机数种子
    
    例子：
    1. 输入表：
    +---+-------+----+-------+
    | id|country|hour|clicked|
    +---+-------+----+-------+
    |  1|     US|  18|    1.0|
    |  2|     CA|  12|    0.0|
    |  3|     EA|  11|    0.0|
    |  4|     CA|  12|    0.0|
    |  5|     UA|  17|    0.0|
    |  6|     CA|  12|    0.0|
    |  7|     NZ|  18|    0.0|
    +---+-------+----+-------+
      
    2. 参数列表:
    {'seed': 123.2, 'left_weight': '0.2', 'right_weight': '0.8'}
    
    3. 输出表:
    
    +---+-------+----+-------+
    | id|country|hour|clicked|
    +---+-------+----+-------+
    |  2|     CA|  12|    0.0|
    |  5|     UA|  17|    0.0|
    +---+-------+----+-------+
    
    
    +---+-------+----+-------+
    | id|country|hour|clicked|
    +---+-------+----+-------+
    |  1|     US|  18|    1.0|
    |  3|     EA|  11|    0.0|
    |  4|     CA|  12|    0.0|
    |  6|     CA|  12|    0.0|
    |  7|     NZ|  18|    0.0|
    +---+-------+----+-------+

'''


class RandomSplitOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1.参数的获取
        left_weight = float_convert(self.conf.get("left_weight"))
        right_weight = float_convert(self.conf.get("right_weight"))
        seed = self.conf.get("seed")
        df = dataframe_list[0]

        # 2.参数的检测
        if seed is None:
            seed = None
        else:
            seed = float_convert(seed)
        check_dataframe(df)

        # 3.随机拆分
        weights = [left_weight, right_weight]
        dataframe = df.randomSplit(weights, seed)
        return dataframe
