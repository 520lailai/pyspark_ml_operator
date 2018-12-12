# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.sql.functions import col
from pyspark.ml.feature import MinMaxScaler
from tools.OperatorsParameterParseUtils import *

""" 
    模块功能：将每一维特征线性地映射到指定的区间[最小值，最大值]，通常可能是[0, 1]
    
    计算公式：
    Rescaled(ei)=[(ei−Emin)/(Emax−Emin)]∗(max−min)+min
    
    输入的列的数据类型可能是：
    double类型、bigint类型、vector类型。
    如果是前两种的数据类型，则进行手动的计算
    如果是vector类型，则调用spark.ml中的MinMaxScaler进行变换
    
    注意因为零值转换后可能变为非零值，所以即便为稀疏输入，输出也可能为稠密向量。
    
    conf 参数：
         "normalize_scaler_conf" ：
          格式：[["输入列名"、"输出列名"、"最小值"，"最大值"、"是否删除输入列"]]
          
    例子：
    1、输入的表：
    +---+-------+----+-------+--------------+
    | id|country|hour|clicked|       fetaure|
    +---+-------+----+-------+--------------+
    |  1|     US|  18|    1.0|[2.7,0.1,-1.0]|
    |  2|     CA|  12|    0.0|[1.6,0.8,-1.0]|
    |  3|     NZ|  15|    0.0|[6.8,6.1,-1.0]|
    +---+-------+----+-------+--------------+
    
    2、conf参数：
    
    {
      'normalize_scaler_conf': 
          [
             ['hour', 'scaler_hour', '0', '1', 'False'], 
             ['clicked', 'scaler_clicked', '10', '20', 'False'], 
             ['fetaure', 'scaler_fetaure', '0', '1', 'False']
         ]
    }
    
    
    3、输出的表：
    
    +---+-------+----+-------+--------------+-----------+--------------+-----------------------------+
    |id |country|hour|clicked|fetaure       |scaler_hour|scaler_clicked|scaler_fetaure               |
    +---+-------+----+-------+--------------+-----------+--------------+-----------------------------+
    |1  |US     |18  |1.0    |[2.7,0.1,-1.0]|1.0        |1.0           |[0.2115384615384616,0.0,0.5] |
    |2  |CA     |12  |0.0    |[1.6,0.8,-1.0]|0.0        |0.0           |[0.0,0.11666666666666668,0.5]|
    |3  |NZ     |15  |0.0    |[6.8,6.1,-1.0]|0.5        |0.0           |[1.0,1.0,0.5]                |
    +---+-------+----+-------+--------------+-----------+--------------+-----------------------------+

"""


class NormalizedOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        normalize_scaler_conf = self.conf.get("normalize_scaler_conf")
        df = dataframe_list[0]
        check_dataframe(df, self.op_id)
        check_parameter_null_or_empty(normalize_scaler_conf, "normalize_scaler_conf", self.op_id)

        type_dict = {}
        for tuple in df.dtypes:
            type_dict[tuple[0]] = tuple[1]

        for i, conf in enumerate(normalize_scaler_conf):
            if len(conf) < 4:
                raise ParameterException("the lengths of parameter must more than 5:" + str(conf)+"opid:"+str(self.op_id))

            input_col = conf[0]
            output_col = conf[1]
            min = conf[2]
            max = conf[3]

            if not min:
                min = 0
            else:
                min = float_convert(min, self.op_id)

            if not max:
                max = 1
            else:
                max = float_convert(max, self.op_id)
            is_drop_input = bool_convert(conf[4], self.op_id)
            check_parameter_null_or_empty(input_col, "input_col", self.op_id)
            check_parameter_null_or_empty(output_col, "output_col", self.op_id)

            type = type_dict[input_col]
            if type == "vector":
                df = self.min_max_scaler(df, input_col, output_col, min, max)
            elif type == 'bigint' or type == 'double':
                max_value = df.agg({input_col: "max"}).collect()[0][0]
                min_value = df.agg({input_col: "min"}).collect()[0][0]
                df = df.withColumn(output_col, self.normalized(col(input_col), max_value, min_value))
            else:
                raise ParameterException("input col must be bigint/double/vector,does not support: " + type+"opid:"+str(self.op_id))

            if is_drop_input:
                df = df.drop(input_col)
        return [df]


    def normalized(self, value, max_value, min_value):
        if max_value == min_value:
            return value
        return (value - min_value) / (max_value - min_value)


    def min_max_scaler(self, df, input_col, output_col, min, max):
        check_parameter_null_or_empty(input_col, "input_col", self.op_id)
        check_parameter_null_or_empty(output_col, "output_col", self.op_id)
        scaler = MinMaxScaler(inputCol=input_col, outputCol=output_col, min=min, max=max)
        scaler_model = scaler.fit(df)
        scaled_data = scaler_model.transform(df)
        return scaled_data
