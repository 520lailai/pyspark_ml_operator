# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.ml.feature import Bucketizer
from pyspark.ml.feature import QuantileDiscretizer
from pyspark.sql.functions import lit
from tools.OperatorsParameterParseUtils import *

""" 
    模块功能： 特征离散分箱，将数值类型的列值分段处理 将连续数值 转换为离散类别。
        目前支持离散的类型：
        (1)等距离散:   isometric_discretization
        需要指定一个离散的值：float数字，表示每个桶的边界大小。
        如果桶的边界大小设置得过大，则会把所有的数据全部放进一个桶里。
        
        (2)等频离散:   isofrequecy_discretization
        需要指定一个离散的值：float数字，表示每个桶的数据量。
        如果每个桶的数据量设置得过大，则会把所有的数据全部放进一个桶里。
        
        (3)自定义离散:  custom_discretization
        需要输入一个List[float]，表示分裂点。
        
        自定义离散注意点：
        list[float] 分裂点列表：分箱数为n+1时，将产生n个区间。
        ex:
        split = [x, y, z] 将产生区间范围为[x,y) 和 [y,z]两个分箱，最后一个分箱包含z.
        分箱必须是严格递增的, 例如: s0 < s1 < s2 < ... < sn，分裂点一般必须大于3个，
        当不确定分裂的上下边界时，应当添加-inf和inf 以涵盖float的所有值,
        否则，指定的分裂点之外的值将被视为错误值。
        
    conf 参数：
    
         "bucketizer_conf" :
          格式：[ ["离散的类型", "离散的值", "计算的列名", "输出的列名","是否删原始的列"] ] 
    
    例子：
    1. 输入表：
        +--------+
        |features|
        +--------+
        |  -999.9|
        |    -0.5|
        |    -0.3|
        |     0.0|
        |     0.2|
        |   999.9|
        +--------+
    
    2. conf配置：
    
       {
          'bucketizer_conf': 
             [
                ['isometric_discretization', '100', 'features', 'features_bucketed', 'True']
             ]
       }
       
    3. 输出表：
        +--------+-----------------+
        |features|features_bucketed|
        +--------+-----------------+
        |  -999.9|              0.0|
        |    -0.5|              9.0|
        |    -0.3|              9.0|
        |     0.0|              9.0|
        |     0.2|             10.0|
        |   999.9|             19.0|
        +--------+-----------------+
"""


class BucketizerOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        bucketizer_conf = self.conf.get("bucketizer_conf")
        df = dataframe_list[0]
        check_dataframe(df, self.op_id)

        # 2、参数解析
        check_parameter_null_or_empty(bucketizer_conf, "bucketizer_conf", self.op_id)

        for conf in bucketizer_conf:
            if len(conf) < 5:
                raise ParameterException("the parameter must more than 5!")
            splits_type = conf[0]
            split = str_convert_floatlist(conf[1], self.op_id)
            input_col = conf[2]
            output_col = conf[3]
            is_drop_input = bool_convert(conf[4], self.op_id)
            # 判空
            check_parameter_null_or_empty(splits_type, "splits_type", self.op_id)
            check_parameter_null_or_empty(split, "split", self.op_id)
            check_parameter_null_or_empty(input_col, "input_col", self.op_id)
            check_parameter_null_or_empty(output_col, "output_col", self.op_id)
            # 判列是否正确
            check_cols([input_col], df.columns, self.op_id)

            # 等频离散
            if splits_type == "isofrequecy_discretization":
                frequecy = split[0]
                if frequecy <= 0:
                    raise ParameterException("[arthur_error] the parameter split(frequecy) must bigger than 0!"+str(self.op_id))
                num_bucket = df.count() / frequecy
                if num_bucket <= 1:
                    df = df.withColumn(output_col, lit(0))
                else :
                    df = self.quantile_discretizer(df, num_bucket, input_col, output_col)
            # 等距离散,自定义离散
            else:
                if splits_type == "isometric_discretization":
                    distance = split[0]
                    if distance <= 0:
                        raise ParameterException("[arthur_error] the parameter split(distance) must bigger than 0!"+str(self.op_id))
                    max_value = df.agg({input_col: "max"}).collect()[0][0]
                    min_value = df.agg({input_col: "min"}).collect()[0][0]
                    split = self.get_bucket_splits(max_value, min_value, distance)

                self.check_split(split)
                if len(split) >= 3:
                    df = self.bucketizer_discretizer(df, split, input_col, output_col)
                elif len(split) == 2:
                    df = df.withColumn(output_col, lit(0))
                else :
                    raise ParameterException("[arthur_error] the length of parameter：split must greater than 1"+str(self.op_id))

            if is_drop_input:
                df.drop(input_col)
        return [df]


    def bucketizer_discretizer(self, df, split, input_col, output_col):
        bucketizer = Bucketizer(splits=split, inputCol=input_col, outputCol=output_col)
        df = bucketizer.transform(df)
        return df

    def quantile_discretizer(self,df, num_bucket, input_col, output_col):
        discretizer = QuantileDiscretizer(numBuckets=num_bucket, inputCol=input_col, outputCol=output_col)
        df = discretizer.fit(df).transform(df)
        return df

    def check_split(self,split):
        '''
        功能：分裂点必须是单调递增的数字系列。
        :param split:
        :return:
        '''
        if not split:
            raise ParameterException("[arthur_error] the parameter：split is null or empty"+str(self.op_id))
        if type(split) != list:
            raise ParameterException("[arthur_error] the parameter：split must be a list type"+str(self.op_id))
        for i in range(1, len(split) - 1):
            if split[i] <= split[i - 1]:
                raise ParameterException("[arthur_error] the parameter：split must Strictly increasing, ex: s0 < s1 < s2 < ... < sn"+str(self.op_id))


    def get_bucket_splits(self, max_value, min_value, distance):
        '''
        功能：获得等距离的分裂点。
        :param max_value: 最大值
        :param min_value:最小值
        :param distance:距离
        :return: 返回一个Liat[float],分裂点列表
        '''
        if min_value > max_value:
            raise ParameterException("[arthur_error] the max_value must bigger than min_value"+str(self.op_id))
        splits = []
        temp = min_value
        while temp < max_value:
            splits.append(temp)
            temp += distance

        splits.append(temp)
        return splits
