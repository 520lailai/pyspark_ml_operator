# -*- coding: utf-8 -*-
from Operator import Operator
from pyspark.ml.feature import OneHotEncoder, StringIndexer

''' 
    conf[]：
       "string_indexer_input_col": String,
        "string_indexer_output_col": String,
        "onehot_encoder_input_col": String,
        "onehot_encoder_output_col": String,
        "drop_last": False 
    dataframe_list: []
'''


class OneHotEncoderEstimatorOperator(Operator):
    def handle(self, dataframe_list, spark):
        string_indexer_input_col = self.conf["string_indexer_input_col"]
        string_indexer_output_col = self.conf["string_indexer_output_col"]
        onehot_encoder_input_col = self.conf["onehot_encoder_input_col"]
        onehot_encoder_output_col = self.conf["onehot_encoder_output_col"]
        drop_last = self.conf["drop_last"]
        df = dataframe_list[0]

        if df:
            stringIndexer = StringIndexer(inputCol=string_indexer_input_col, outputCol=string_indexer_output_col)
            model = stringIndexer.fit(df)
            indexed = model.transform(df)
            encoder = OneHotEncoder(dropLast=drop_last, inputCol=onehot_encoder_input_col,
                                    outputCol=onehot_encoder_output_col)
            encoded = encoder.transform(indexed)
            self.result_type = "single"
            self.status = "finished"
            return [encoded]
        else:
            raise ValueError
