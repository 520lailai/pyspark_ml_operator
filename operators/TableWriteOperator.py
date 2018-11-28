# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

'''
    conf[]
        db_name:      String,
        table_name:   String,
        partition_by: String,  names of partitioning columns
    spark: SparkSession
'''


class TableWriteOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        db_name = self.conf["db_name"]
        table_name = self.conf["table_name"]
        save_format = "parquet"
        mode = "overwrite"
        partition_by = self.conf["partition_by"]

        check_str_parameter(db_name, "the parameter:db_name is null!")
        check_str_parameter(table_name, "the parameter:table_name is null!")
        name = db_name + "." + table_name

        check_dataframe(dataframe_list)

        if not partition_by:
            partition_by = None

        for df in dataframe_list:
            df.write.saveAsTable(name, format=save_format, mode=mode, partitionBy=partition_by)
        return dataframe_list
