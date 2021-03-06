# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from backend.framework.tools.OperatorsParameterParseUtils import *

"""  
    模块功能： 读取数据算子
    conf 参数：
         "db_name": String,       数据库名
         "table_name": String,    表名
         "partition_by": String, 分区，格式：列名=值， ex:p_date='20181015' 
         "limit_num": 限制条数
    例子：
    
   
    1、cof参数配置：
    conf_read1 = {"db_name": "lai_test",
                  "table_name": "test1",
                  "limit": "2",
                  "partition_by": None};
    2、结果表：
     +---+-------+----+-------+
    |id |country|hour|clicked|
    +---+-------+----+-------+
    |9  |NZ     |15  |0.0    |
    |8  |CA     |12  |0.0    |
    +---+-------+----+-------+
    
    
"""


class TableReadOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        db_name = self.conf.get("db_name")
        table_name = self.conf.get("table_name")
        partition_by = self.conf.get("partition_by")
        limit_num = self.conf.get("limit_num")

        check_parameter_null_or_empty(db_name, "db_name", self.op_id)
        check_parameter_null_or_empty(table_name, "table_name", self.op_id)

        # 1. 判断是否为partition的表
        df = spark.sql("desc formatted " + db_name + "." + table_name)
        partition_count = df.select(df['col_name']).filter(df["col_name"] == "# Partition Information").count()

        # 2. 如果是partition的表必须有partition_by
        if not partition_by and partition_count != 0:
            raise partitionValException("[arthur_error] partition table must have partitionVal!")

        # 3. sql拼接
        sql = "select * from " + db_name + "." + table_name
        if partition_by:
            sql += " where " + partition_by

        if limit_num:
            sql += " limit " + str(limit_num)
        try:
            # 4. query查询
            dataframe = spark.sql(sql)
            return [dataframe]
        except Exception as e:
            e.args += (' op_id :' + str(self.op_id),)
            raise


class partitionValException(BaseException):
    def __init__(self, mesg="[arthur_error] partition table must have partition value"):
        print(mesg)
