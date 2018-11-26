# -*- coding: utf-8 -*-
from Operator import Operator
from OperatorsUtils import *

'''
    conf[]
        db_name:      String,
        table_name:   String,
        save_format:  String,  the format used to save
        mode:         String,  one of `append`, `overwrite`, `error`, `ignore` (default: error)
        partition_by: String,  names of partitioning columns
        options:      String:(k1=v1,k2=v2.....)  all other string options
    spark: SparkSession
'''


class TableWriteOperator(Operator):

    def handle(self, dataframe_list, spark):
        db_name = self.conf["db_name"]
        table_name = self.conf["table_name"]
        save_format = self.conf["save_format"]
        mode = self.conf["mode"]
        partition_by = self.conf["partition_by"]
        options = self.conf["options"]

        check_str_parameter(db_name, "the parameter:db_name is null!")
        check_str_parameter(table_name, "the parameter:table_name is null!")
        name = db_name + "." + table_name

        check_dataframe(dataframe_list)

        if not mode:
            mode = 'overwrite'

        if not partition_by:
            partition_by = None

        for df in dataframe_list:
            if options:
                df.write.saveAsTable(name, format=save_format, mode=mode, partitionBy=partition_by, options=options)
            else:
                df.write.saveAsTable(name, format=save_format, mode=mode, partitionBy=partition_by)

        return dataframe_list
