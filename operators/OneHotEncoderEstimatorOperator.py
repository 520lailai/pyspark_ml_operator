# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import *
from tools.OperatorsParameterParseUtils import *
import traceback

""" 
    模块功能： 对分类型的特征进行独热码编码
    
    One-Hot编码，又称为一位有效编码，主要是采用位状态寄存器来对个状态进行编码，
    每个状态都由独立的寄存器位，并且在任意时候只有一位有效。
    
    onehot编码是可以指定handle_invalid， “keep” 无效数据会表示为额外的分类特征，“error” 抛出错误 。

    conf 参数：
        "onehot_conf": [[]]               输入输出列名 ：["输入列名"，"输出列名"]
        "drop_last": bool                 是否删除最后一个枚举量
        "handle_invalid" :String          如何处理无效数据， "keep"，"error" 
        "other_col_output": List[String]  需要添加到结果表中的其他的列(除了编过码的列）
        "is_output_model": bool,default=True 是否输出StringIndex的模型表
    
    例子：
    
    1、原始表：
    +---+----------------+----+-----+-------+
    | id|         country|hour|score|clicked|
    +---+----------------+----+-----+-------+
    |  1|           China|  18|  1.5|      2|
    |  2|         America|  12|  0.0|      4|
    |  3|          Brazil|   5|  0.5|      5|
    |  4|united kiongdom |   4|  6.7|      9|
    |  5|         Vietnam|  15|  0.0|      5|
    +---+----------------+----+-----+-------+ 
    
    2、conf 参数的配置
    {
        'handle_invalid': 'keep', 
        'onehot_conf': [['country', 'country_onehot'], ['hour', 'hour-onehot'], ['score', 'score-onehot']], 
        'other_col_output': ['id', 'clicked'],
        'is_output_model': True, 'drop_last': True
    }
    
    3、输出的表
    
    结果表：
    +---+-------+--------------+---------------+-------------+
    |id |clicked|country_onehot|hour-onehot    |score-onehot |
    +---+-------+--------------+---------------+-------------+
    |1  |2      |(6,[0],[1.0]) |(19,[18],[1.0])|(5,[2],[1.0])|
    |2  |4      |(6,[4],[1.0]) |(19,[12],[1.0])|(5,[0],[1.0])|
    |3  |5      |(6,[2],[1.0]) |(19,[5],[1.0]) |(5,[1],[1.0])|
    |4  |9      |(6,[1],[1.0]) |(19,[4],[1.0]) |(5,[3],[1.0])|
    |5  |5      |(6,[3],[1.0]) |(19,[15],[1.0])|(5,[0],[1.0])|
    +---+-------+--------------+---------------+-------------+
    模型表：
    +--------+----------------+-------+
    |col_name|col_value       |mapping|
    +--------+----------------+-------+
    |country |united kiongdom |1.0    |
    |country |China           |0.0    |
    |country |Vietnam         |3.0    |
    |country |Brazil          |2.0    |
    |country |America         |4.0    |
    |score   |6.7             |3.0    |
    |score   |0.5             |1.0    |
    |score   |1.5             |2.0    |
    |score   |0.0             |0.0    |
    +--------+----------------+-------+
"""


class OneHotEncoderEstimatorOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        # 1、参数的解析与检测
        onehot_conf = self.conf.get("onehot_conf")
        drop_last = bool_convert(self.conf.get("drop_last", True), self.op_id)
        handle_invalid = self.conf.get("handle_invalid", "keep")
        other_col_output = self.conf.get("other_col_output")
        is_output_model = bool_convert(self.conf.get("is_output_model", True), self.op_id)
        if not other_col_output:
            other_col_output = []
        else:
            other_col_output = str_convert_strlist(other_col_output, self.op_id)

        check_parameter_null_or_empty(onehot_conf, "onehot_conf", self.op_id)
        check_parameter_null_or_empty(handle_invalid, "handle_invalid", self.op_id)

        # 1.1、获得输入的表，和模型表
        df = dataframe_list[0]
        check_dataframe(df, self.op_id)
        if len(dataframe_list) >= 2:
            input_modle = dataframe_list[1]
            check_dataframe(input_modle, self.op_id)
        else:
            input_modle = None

        input_cols = []
        output_cols = []
        for conf in onehot_conf:
            input_cols.append(conf[0])
            output_cols.append(conf[1])

        check_parameter_null_or_empty(input_cols, "input_col", self.op_id)
        check_parameter_null_or_empty(output_cols, "output_col", self.op_id)

        # 2、获得表的schema
        dtype = df.dtypes
        col_type = {}
        for name in dtype:
            col_type[name[0]] = name[1]

        # 3、StringIndex的编码
        if input_modle:
            df, input_cols = self.string_index_from_model(input_cols, df, input_modle, col_type)
        else:
            for i, col in enumerate(input_cols):
                if col_type[col] == 'string' or col_type[col] == 'double':
                    indexer = StringIndexer(inputCol=col, outputCol=col + "_arthur_index",
                                            handleInvalid=handle_invalid, stringOrderType="alphabetDesc")
                    df = indexer.fit(df).transform(df)
                    input_cols[i] = col + "_arthur_index"

        # 4、onehot encoder
        encoder = OneHotEncoderEstimator(inputCols=input_cols, outputCols=output_cols)
        if drop_last is not None:
            drop_last = bool_convert(drop_last, self.op_id)
            encoder.setDropLast(drop_last)
        if handle_invalid:
            encoder.setHandleInvalid(handle_invalid)
        model = encoder.fit(df)
        encoded = model.transform(df)

        # 5、获得输出模型表
        output_model = None
        if is_output_model:
            if input_modle:
                output_model = input_modle
            else:
                output_model = self.get_output_model(df, input_cols, spark)

        # 6、获得输出表
        for name in encoded.columns:
            if name not in other_col_output and name not in output_cols:
                encoded = encoded.drop(name)

        return [encoded, output_model]


    def string_index_from_model(self, input_cols, df, modle, col_type):
        '''
        用模型表 对输入表进行StringIndex编码
        :param input_cols: 编码的列名
        :param df: dataframe
        :param modle: 模型表
        :param col_type: dataframe的schema
        :return: df, input_cols
        '''
        if not input_cols or not df or not modle or not col_type:
            msg = traceback.format_exc()
            print(msg)
            raise ParameterException("function:string_index_from_model, check parameter error,opid:"+str(self.op_id))

        if modle.count() <= 0:
            raise ParameterException("the input modle table is empty~")

        for i, input in enumerate(input_cols):
            # 1、filter model
            temp_modle = modle.filter(modle["col_name"] == input).select("col_value", "mapping")
            if temp_modle and temp_modle.count() > 0:
                # 2、col_value type cast
                temp_modle = temp_modle.withColumn("col_value", temp_modle["col_value"].cast(col_type[input]))
                # 3、 join, (add a mapping col)
                df = df.join(temp_modle, df[input] == temp_modle["col_value"], "left").drop("col_value")
                df = df.withColumnRenamed("mapping", input + "_arthur_index")
                input_cols[i] = input + "_arthur_index"
        return df, input_cols


    def get_output_model(self, df, input_cols, spark):
        '''
        从dataframe中提取StringIndex的映射表
        :param df: dataframe
        :param input_cols: onehot编码的列
        :param spark: SparkSession
        :return: dataframe
        '''
        if not input_cols or not df or not spark:
            msg = traceback.format_exc()
            print(msg)
            raise ParameterException("function:get_output_model, check parameter error")

        # 1、构建模型表的schema
        fields = []
        fields.append(StructField("col_name", StringType(), True))
        fields.append(StructField("col_value", StringType(), True))
        fields.append(StructField("mapping", DoubleType(), True))
        schema = StructType(fields)

        # 2、组装模型表
        model_list = []
        for col in input_cols:
            if "_arthur_index" in col:
                col_name = col[:-13]
                if col_name not in df.columns:
                    print("the df not have col_name: ", col_name)
                    continue
                temp = df.select(col_name, col).distinct()
                if not temp or temp.count == 0:
                    continue
                for row in temp.collect():
                    model_list.append((col_name, str(row[col_name]), row[col]))

        return spark.createDataFrame(model_list, schema)
