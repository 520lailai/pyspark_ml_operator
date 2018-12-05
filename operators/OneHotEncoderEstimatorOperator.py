# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.ml.feature import StringIndexer
from OperatorsUtils import *
from pyspark.sql.types import *

''' 
    conf[]：
        onehot_conf : [input_col[String], output_col[String]]
        drop_last: bool
        handle_invalid :String
        other_col_output: List[String]
        is_output_model : bool True
    dataframe_list: []
'''


class OneHotEncoderEstimatorOperator(DataProcessingOperator):
    def handle(self, dataframe_list, spark):
        # 1、parse parameter
        onehot_conf = self.conf["onehot_conf"]
        drop_last = bool_convert(self.conf["drop_last"])
        handle_invalid = self.conf["handle_invalid"]
        other_col_output = str_convert_strlist(self.conf["other_col_output"])
        is_output_model = bool_convert(self.conf["is_output_model"])

        df = dataframe_list[0]
        check_dataframe(df)
        if len(dataframe_list) >= 2:
            input_modle = dataframe_list[1]
            check_dataframe(input_modle)
        else:
            input_modle = None

        input_cols = []
        output_cols = []
        for conf in onehot_conf:
            input_cols.append(conf[0])
            output_cols.append(conf[1])
            check_str_parameter(input_cols, "the parameter:input_col is null!")
            check_str_parameter(output_cols, "the parameter:output_col is null!")

        print("1、parse parameter", "input_cols:", input_cols, "output_cols:",output_cols)

        # 2、get col_type
        dtype = df.dtypes
        col_type = {}
        for name in dtype:
            col_type[name[0]] = name[1]
        print("2、get col_type", col_type)

        # 3、index encoder
        if input_modle:
            df, input_cols = string_index_from_model(input_cols, df, input_modle, col_type)
        else:
            for i, col in enumerate(input_cols):
                if col_type[col] == 'string' or col_type[col] == 'double':
                    indexer = StringIndexer(inputCol=col, outputCol=col + "_arthur_index")
                    df = indexer.fit(df).transform(df)
                    input_cols[i] = col + "_arthur_index"
        print("3、index encoder", "input_cols", input_cols)

        # 4、onehot encoder
        encoder = OneHotEncoderEstimator(inputCols=input_cols, outputCols=output_cols)
        if drop_last is not None:
            drop_last = bool_convert(drop_last)
            encoder.setDropLast(drop_last)
        if handle_invalid:
            encoder.setHandleInvalid(handle_invalid)
        model = encoder.fit(df)
        encoded = model.transform(df)

        print("4、onehot encoder")

        # 5、get output model
        output_model = None
        if is_output_model:
            if input_modle:
                output_model = input_modle
            else:
                output_model = get_output_model(df, input_cols, spark)
        print("5、get output model")

        # 6、get output table
        for name in encoded.columns:
            if name not in other_col_output and name not in output_cols:
                encoded = encoded.drop(name)

        print("6、get output table")

        return [encoded, output_model]


def string_index_from_model(input_cols, df, modle, col_type):
    if modle.count() <= 0:
        raise ParameterException("the input modle table is empty~")
    for i, input in enumerate(input_cols):
        # 1、filter model
        temp_modle = modle.filter(modle["col_name"] == input).select("col_value", "mapping")
        if temp_modle.count() > 0:
            # 2、col_value type cast
            temp_modle = temp_modle.withColumn("col_value", temp_modle["col_value"].cast(col_type[input]))
            # 3、 join, (add a mapping col)
            df = df.join(temp_modle, df[input] == temp_modle["col_value"], "left").drop("col_value")
            df = df.withColumnRenamed("mapping", input + "_arthur_index")
            input_cols[i] = input + "_arthur_index"
    return df, input_cols


def get_output_model(df, input_cols, spark):
    fields = []
    fields.append(StructField("col_name", StringType(), True))
    fields.append(StructField("col_value", StringType(), True))
    fields.append(StructField("mapping", DoubleType(), True))
    schema = StructType(fields)
    model_list = []
    for col in input_cols:
        if "_arthur_index" in col:
            col_name = col[:-13]
            if col_name not in df.columns:
                print("the df not have col_name: ", col_name)
                continue
            temp = df.select(col_name, col).distinct()
            if not temp or temp.count == 0:
                continue
            for row in temp.collect():
                model_list.append((col_name, str(row[col_name]), row[col]))
    return spark.createDataFrame(model_list, schema)
