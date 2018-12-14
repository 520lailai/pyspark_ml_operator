# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *

from pyspark.sql.types import *
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors as MLLibVectors


"""
 模块功能：将带有标签的数据转换成libsvm 格式
    label：数值类型
    feature：向量类型
    假设有一个dataframe有两列 ["label", "features"]
    其中features是向量类型。
    算子的作用 ：把dataframe的数据 转换成libsvm 格式。
    分为两步：先给向量打标签 → 再转换
    
    conf 参数：
       "column_name": String, 特征列名  ex: "feature"
       "label":       String, 标签列名  ex: "id"
       "output":      String, 输出列名  ex: "libsvm_feature"
    
    例子：
    
    1、输入的表：
    +---+-------+----+---------------+
    | id|country|hour|        clicked|
    +---+-------+----+---------------+
    |  1|     US|  18|[1.0, 2.9, 4.5]|
    |  2|     CA|  12|[2.0, 1.3, 7.1]|
    |  3|     NZ|  15|[3.0, 2.6, 6.3]|
    +---+-------+----+---------------+
    
    2、参数：
    
    {'output': 'label_libsvm_clicked', 'label': 'id', 'column_name': 'clicked'}
    
    3、输出的表：
    +---------------------+
    |label_libsvm_clicked |
    +---------------------+
    |1.0 1:1.0 2:2.9 3:4.5|
    |2.0 1:2.0 2:1.3 3:7.1|
    |3.0 1:3.0 2:2.6 3:6.3|
    +---------------------+
"""


class LabelFeatureToLibsvmOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数的获取
        column_name = self.conf.get("column_name")
        label = self.conf.get("label")
        output = self.conf.get("output")
        df = dataframe_list[0]

        # 2、参数的检测
        check_dataframe(df, self.op_id)
        check_parameter_null_or_empty(column_name, "column_name", self.op_id)
        check_parameter_null_or_empty(label, "label", self.op_id)
        check_parameter_null_or_empty(output, "output", self.op_id)

        # 3、映射转化
        rdd = df.rdd.map(lambda row: self.map_function(row, label, column_name, output))
        df = spark.createDataFrame(rdd)
        return [df]

    def map_function(self, row, label, col_name, output_col):
        '''
        给定一个label和一个feature，转化为一个libsvm格式
        :param row: dataframe的row
        :param label: 标签列名
        :param feature: 特征列名
        :return:
        '''
        feature = row[col_name]
        type_feature = str(type(feature))

        # 1、label的类型检查，必须为可以通过float()转化的类型
        try:
            float(row[label])
        except ValueError:
            raise ParameterException("the input label type must could convert to a float, op_id:" + str(self.op_id))

        # 2、feature的类型检查，必须为LabeledPoint支持的Local Vector的类型
        if "pyspark.ml.linalg.SparseVector" in type_feature or "pyspark.ml.linalg.DenseVector" in type_feature:
            feature = MLLibVectors.fromML(feature)

        elif type_feature.find("mllib.linalg.SparseVector") == -1 and type_feature.find(
                "mllib.linalg.DenseVector") == -1 and type_feature.find("list") == -1 and type_feature.find(
            'numpy.ndarray') == -1:
            raise ParameterException(
                "the input vector type is not a support type:" + type_feature + ", opid" + str(self.op_id))

        try:
            # 3、先转化为LabeledPoint
            pos = LabeledPoint(row[label], feature)

            # 4、再转化为libsvm格式
            str_libsvm = MLUtils._convert_labeled_point_to_libsvm(pos)

            if str_libsvm:
                return {output_col: str_libsvm}
            else:
                return {output_col: ""}

        except Exception as e:
            e.args += (' op_id :' + str(self.op_id),)
            raise
