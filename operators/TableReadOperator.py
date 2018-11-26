# -*- coding: utf-8 -*-
from Operator import Operator
import logging
from OperatorsUtils import *

''' conf[]：db_name:String, table_name:String, partition_val:String, 
    spark: SparkSession'''


class TableReadOperator(Operator):
    def handle(self, dataframe_list, spark):
        db_name = self.conf["db_name"]
        table_name = self.conf["table_name"]
        partition_val = self.conf["partition_val"]

        check_str_parameter(db_name, "the parameter:db_name is null!")
        check_str_parameter(table_name, "the parameter:table_name is null!")

        # 1. is partiton table ?
        df = spark.sql("desc formatted " + db_name + "." + table_name)
        partition_count = df.select(df['col_name']).filter(df["col_name"] == "# Partition Information").count()

        # 2.  partion table must have partition_val
        if not partition_val and partition_count != 0:
            raise partitionValException("partition table must have partitionVal!")

        # 3. sql
        sql = "select * from " + db_name + "." + table_name
        if partition_val:
            sql += " where " + partition_val

        # 4. query
        dataframe = spark.sql(sql)
        return [dataframe]


class partitionValException(BaseException):
    def __init__(self, mesg="partition table must have partition value"):
        logging(mesg)
