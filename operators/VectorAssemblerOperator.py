# -*- coding: utf-8 -*-
from pyspark.ml.feature import VectorAssembler
from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *

"""  
    模块功能： 对部分数值类型的列的数据合并转换为一个特征向量，并统一命名。
    conf 参数：
       "column_names": List[String],聚合列名  ex:  ["id1","hour1","clicked1"]
       "output": String, 输出列名
    例子：
    
    1、输入表：
    +---+-------+----+-------------+
    | id|country|hour|      clicked|
    +---+-------+----+-------------+
    |  1|     US|  18|[1.0,2.9,4.5]|
    |  2|     CA|  12|[2.0,1.3,7.1]|
    |  3|     NZ|  15|[3.0,2.6,6.3]|
    +---+-------+----+-------------+
    
    2、cof参数配置：
    
    {'output': 'label_libsvm_clicked', 'column_names': ['hour', 'clicked']}
    
    3、结果表：
    +---+-------+----+-------------+--------------------+
    |id |country|hour|clicked      |label_libsvm_clicked|
    +---+-------+----+-------------+--------------------+
    |1  |US     |18  |[1.0,2.9,4.5]|[18.0,1.0,2.9,4.5]  |
    |2  |CA     |12  |[2.0,1.3,7.1]|[12.0,2.0,1.3,7.1]  |
    |3  |NZ     |15  |[3.0,2.6,6.3]|[15.0,3.0,2.6,6.3]  |
    +---+-------+----+-------------+--------------------+
"""


class VectorAssemblerOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数的获取
        column_names = self.conf.get("column_names")
        output = self.conf.get("output")
        df = dataframe_list[0]
        # 2、参数的检查
        check_dataframe(df, self.op_id)
        check_strlist_parameter(column_names, self.op_id)
        check_parameter_null_or_empty(output, "output", self.op_id)

        try :
            # 2、特征聚合
            assembler = VectorAssembler(inputCols=column_names, outputCol=output)
            df = assembler.transform(df)
            return [df]

        except Exception as e:
            e.args += (' op_id :' + str(self.op_id))
            raise
