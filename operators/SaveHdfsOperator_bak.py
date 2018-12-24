# -*- coding: utf-8 -*-

from DataProcessingOperator import DataProcessingOperator


class SaveHdfsOperator(DataProcessingOperator):

    def handle(self, dataframe_list, spark):
        df = dataframe_list[0]
        config = self.conf
        if "output" in config:
            output = config["output"]
        else:
            raise Exception("[arthur_error] Config parse error , operator %s no output " % self.op_id)
        
        if df:
             df.show()
             df.rdd.repartition(1).toDF().write.mode("overwrite").csv(path=output, quote="", sep=" ")
             #df.rdd.repartition(1).saveAsTextFile(output)
             #df.rdd.repartition(1).toDF().write.mode("overwrite").csv(path=output,quote="",sep=" ")
        else:
            raise Exception("[arthur_error] input dataframe required not None")
        return [df]

