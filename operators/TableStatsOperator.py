# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from backend.framework.tools.OperatorsParameterParseUtils import *

"""  
    模块功能： 全表的简单统计, 指定某些列，展示这些列的一些列的5个统计信息。 
    用于整体数值特征：最大值，最小值，均值，标准差,统计数，分位：p25，p50,p75
    conf 参数：
         "cols": String 列名列表，如果输入为None， 将会统计所有的数值型的列。
    例子：
    
    1、输入表：
    +---+-------+----+-------+
    | id|country|hour|clicked|
    +---+-------+----+-------+
    |  1|     US|  18|   10.0|
    |  2|     CA|  12|   20.0|
    |  3|     NZ|  15|   30.0|
    |  4|     HK|  19|   30.0|
    |  5|     MZ|  21|   30.0|
    +---+-------+----+-------+
    
    2、cof参数配置：
    
       {'cols': ['hour', 'clicked']}
    
    3、结果表：
    +---+--------+-----+----+------------------+----+----+----+----+----+
    | id|col_name|count|mean|            stddev| min| 25%| 50%| 75%| max|
    +---+--------+-----+----+------------------+----+----+----+----+----+
    |  0|    hour|    5|17.0|3.5355339059327373|  12|  15|  18|  19|  21|
    |  1| clicked|    5|24.0|  8.94427190999916|10.0|20.0|30.0|30.0|30.0|
    +---+--------+-----+----+------------------+----+----+----+----+----+

"""


class TableStatsOperator(DataProcessingOperator):
    schema = ["id", "col_name", "count", "mean", "stddev", "min", "25%", "50%", "75%", "max"]

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        cols = self.conf.get("cols")
        check_parameter_null_or_empty(dataframe_list, "dataframe_list", self.op_id)
        df = dataframe_list[0]

        # 2、参数检查
        check_dataframe(df, self.op_id)

        try:
            # 3、全表统计
            if cols is None:
                dataframe = df.summary()
            else:
                check_strlist_parameter(cols, "cols", self.op_id)
                dataframe = df.select(cols).summary()
            dataframe = dataframe.drop("summary")
            cols = dataframe.columns
            cols_num = len(cols)
            row_num = dataframe.count()
            matrixs = [["null"]*(row_num+2) for i in range(cols_num)]
            rows = dataframe.collect()
            for j, row in enumerate(rows):
                for i in range(0, cols_num):
                    matrixs[i][j+2] = row[i]
            for i in range(0, cols_num):
                matrixs[i][1] = cols[i]
                matrixs[i][0] = i
            dataset = spark.createDataFrame(matrixs, self.schema)
            return [dataset]
        except Exception as e:
            e.args += (' op_id :'+ str(self.op_id),)
            raise

