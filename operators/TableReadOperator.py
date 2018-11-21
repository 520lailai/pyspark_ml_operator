# -*- coding: utf-8 -*-
from Operator import Operator

''' conf[]：db_name:String, table_name:String, partition_val:String, 
    spark: SparkSession'''


class TableReadOperator(Operator):
    def handle(self, dataframe_list, spark):
        db_name = self.conf["db_name"]
        table_name = self.conf["table_name"]
        partition_val = self.conf["partition_val"]

        # 1. is partiton table ?
        df = spark.sql("desc formatted " + db_name + "." + table_name)
        partition_count = df.select(df['col_name']).filter(df["col_name"] == "# Partition Information").count()

        # 2.  partion table must have partition_val
        if (partition_count != 0 and not partition_val):
            raise partitionValException("partition table must have partitionVal!")

        # 3. sql
        sql = "select * from " + db_name + "." + table_name
        if partition_val:
            sql += " where " + partition_val
        sql += " limit 100"

        # 4. query
        dataframe = spark.sql(sql)

        self.result_type = "single"
        self.status = "finished"

        return [dataframe]


class partitionValException(BaseException):
    def __init__(self, mesg="partition table must have partition value"):
        logging mesg
