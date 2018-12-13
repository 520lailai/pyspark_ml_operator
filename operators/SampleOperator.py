# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *

""" 
    模块功能:随机采样，用户指定是否是有放回的抽样，采样比例和随机数种子，生成一个随机抽样样本。
    conf 参数：
         "with_replacement": boolean, 是否有放回的采样  ex:"False"
         "fraction":         float,   数据的采样比例    ex:"0.98"  
         "seed":             float,   随机数种子       ex:"243244"
    

    1、输入表：
    +---+-------+----+-------+
    | id|country|hour|clicked|
    +---+-------+----+-------+
    |  1|     US|  18|    1.0|
    |  2|     CA|  12|    5.0|
    |  3|     CA|  12|    0.0|
    |  4|     CA|  14|    7.0|
    |  5|     SA|  12|    0.0|
    |  6|     BA|  16|    3.0|
    |  7|     UA|  12|    0.0|
    |  8|     OA|  18|    5.0|
    |  9|     PZ|  15|    0.0|
    +---+-------+----+-------+
    
    2、cof配置参数：
    
    conf: {'seed': '325.4', 'with_replacement': True, 'fraction': '0.6'}
    
    3、输出表：
   +---+-------+----+-------+
    |id |country|hour|clicked|
    +---+-------+----+-------+
    |2  |CA     |12  |5.0    |
    |2  |CA     |12  |5.0    |
    |3  |CA     |12  |0.0    |
    |4  |CA     |14  |7.0    |
    |6  |BA     |16  |3.0    |
    |6  |BA     |16  |3.0    |
    |6  |BA     |16  |3.0    |
    |7  |UA     |12  |0.0    |
    |8  |OA     |18  |5.0    |
    |8  |OA     |18  |5.0    |
    |9  |PZ     |15  |0.0    |
    +---+-------+----+-------+
"""


class SampleOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        with_replacement = bool_convert(self.conf.get("with_replacement", False), self.op_id)
        fraction = float_convert(self.conf.get("fraction"), self.op_id)
        seed = self.conf.get("seed")
        df = dataframe_list[0]

        # 2、参数检查
        check_parameter_null_or_empty(fraction, "fraction", self.op_id)
        check_dataframe(df, self.op_id)

        if seed is None:
            seed = None
        else:
            seed = float_convert(seed, self.op_id)

        try:
            # 3、随机采样
            dataframe = df.sample(with_replacement, fraction, seed)
            return [dataframe]
        except Exception as e:
            e.args += (' op_id :' + str(self.op_id))
            raise
