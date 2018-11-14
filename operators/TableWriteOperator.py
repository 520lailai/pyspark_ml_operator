# -*- coding: utf-8 -*-
from Operator import Operator

'''
    conf[]
        db_name:String,
        table_name:  String,
        save_format: String, the format used to save
        mode:   String, one of `append`, `overwrite`, `error`, `ignore` (default: error)
        partition_by: String, names of partitioning columns
        options: String:(k1=v1,k2=v2.....)  all other string options
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
        name = db_name + "." + table_name

        if not partition_by:
            partition_by = None

        for df in dataframe_list:
            if options:
                df.write.saveAsTable(name, format=save_format, mode=mode, partitionBy=partition_by, options=options)
            else:
                df.write.saveAsTable(name, format=save_format, mode=mode, partitionBy=partition_by)

        self.result_type = "single"
        self.status = "finished"

        return None
