# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from tools.OperatorsParameterParseUtils import *
from pyspark.sql.functions import lit
import time

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
        is_partition_by_date = self.conf.get("is_partition_by_date")
        df = dataframe_list[0]

        # 2、参数检查
        check_dataframe(df, self.op_id)
        check_strlist_parameter(partition_by, self.op_id)
        check_parameter_null_or_empty(db_name, "db_name", self.op_id)
        check_parameter_null_or_empty(table_name, "table_name", self.op_id)
        name = db_name + "." + table_name

        try :
            # 按照已有的列做分区
            if partition_by:
                check_cols(partition_by, df.columns)
                df.write.saveAsTable(name, format=save_format, mode=mode, partitionBy=partition_by)
            # 自动新加一列作为partition列名
            elif is_partition_by_date:
                df = df.withColumn("arthur_p_date", lit(time.strftime("%Y-%m-%d|%H:%M:%S", time.localtime())))
                df.write.saveAsTable(name, format=save_format, mode=mode, partitionBy="arthur_p_date")
            else:
                #不分区
                df.write.saveAsTable(name, format=save_format, mode=mode)
            return [df]
        except Exception as e:
            e.args += ' op_id :'+ str(self.op_id)
            raise
