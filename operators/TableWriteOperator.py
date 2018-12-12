# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *
from pyspark.sql.functions import lit

"""  
    模块功能： 把dataframe写到目标表中
    conf 参数：
        "db_name":      String, 数据库名称
        "table_name":   String, 表名称
        "partition_by": String, partition的列名, ex: p_date='20181015'
    例子：
    
    1、输入表：
    +---+------+---+------+
    | id|  name|age|   sex|
    +---+------+---+------+
    |  1|lailai| 18|female|
    |  2| guguo| 12|female|
    |  3|  lili| 15|female|
    +---+------+---+------+
    
    2、cof参数配置：
    conf_write = {
                    "db_name": "lai_test",
                    "table_name": "test_save_new",
                    "partition_by": "p_date='20181015'"
                  };
    
    3、结果表：
"""


class TableWriteOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        # 1、参数获取
        db_name = self.conf.get("db_name")
        table_name = self.conf.get("table_name")
        save_format = "parquet"
        mode = "overwrite"
        partition_by = self.conf.get("partition_by")
        df = dataframe_list[0]

        # 2、参数检查
        check_dataframe(df, self.op_id)
        check_parameter_null_or_empty(db_name, "db_name", self.op_id)
        check_parameter_null_or_empty(table_name, "table_name", self.op_id)
        name = db_name + "." + table_name

        if partition_by:
            try:
                partition_values = partition_by.split("=")
            except Exception:
                raise ParameterException(
                    "the format partition_val is error, must be partirion_name = values, and the value will treated as string, op_id:" + self.op_id)
            check_strlist_parameter(partition_values, self.op_id)
            if partition_values[0].strip not in df.columns:
                df = df.withColumn(partition_values[0].strip(), lit(partition_values[1].strip()))
            # 3、写表
            df.write.saveAsTable(name, format=save_format, mode=mode, partitionBy=partition_values[0])
        else:
            # 3、写表
            df.write.saveAsTable(name, format=save_format, mode=mode)

        return dataframe_list
