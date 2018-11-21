# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import logging

from TableReadOperator import TableReadOperator
from TableWriteOperator import TableWriteOperator
from SampleOperator import SampleOperator
from DefaultValueFillOperator import DefaultValueFillOperator
from MinMaxScalerOperator import MinMaxScalerOperator
from StandardScalerOperator import StandardScalerOperator
from JoinOperator import JoinOperator
from RandomSplitOperator import RandomSplitOperator
from BucketizerOperator import BucketizerOperator
from FeatureExceptionSmoothOperator import FeatureExceptionSmoothOperator
from OneHotEncoderEstimatorOperator import OneHotEncoderEstimatorOperator
from ApplyQuerySqlOperator import ApplyQuerySqlOperator
from MathFunctionsOperator import MathFunctionsOperator
from ApproxQuantileOperator import ApproxQuantileOperator
from TableStatsOperator import TableStatsOperator
from SelectOperator import SelectOperator
from TableToKVOperator import TableToKVOperator


def testTableReadOperator(spark):
    conf_read = {"db_name": "lai_test",
                 "table_name": "test1",
                 "partition_val": None};
    operator = TableReadOperator(op_id="123", op_type="readtable")
    operator.conf = conf_read
    dataset_list = operator.handle([], spark)
    logging ("-----------------1、TableReadOperator")
    dataset_list[0].show()


def testTableWriteOperator(spark):
    conf_write = {"db_name": "lai_test",
                  "table_name": "test_save_new",
                  "save_format": None,
                  "mode": "overwrite",
                  "partition_by": "id",
                  "options": None};

    operator = TableWriteOperator(op_id="123", op_type="readtable")
    operator.conf = conf_write
    dataset = spark.createDataFrame(
        [(1, "lailai", 18, "female"),
         (2, "guguo", 12, "female"),
         (3, "lili", 15, "female")],
        ["id", "name", "age", "sex"])
    logging ("-----------------2、testTableWriteOperator")
    dataset.show()
    operator.handle([dataset], spark)
    logging ("testTableWriteOperator finish")


def testSampleOperator(spark):
    conf = {"with_replacement": True, "fraction": 0.6, "seed": 325.4};
    operator = SampleOperator(op_id="123", op_type="readtable")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 5.0),
         (3, "CA", 12, 0.0),
         (4, "CA", 14, 7.0),
         (5, "SA", 12, 0.0),
         (6, "BA", 16, 3.0),
         (7, "UA", 12, 0.0),
         (8, "OA", 18, 5.0),
         (9, "PZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    logging ("-----------------3、testSampleOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    dataset_list[0].show()


def testDefaultValueFillOperator(spark):
    conf = {"value": {"country": "China", "hour": 100, "clicked": 99.99}, "subset": None};
    operator = DefaultValueFillOperator(op_id="123", op_type="readtable")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, None),
         (3, "CA", 12, 0.0),
         (4, "CA", 14, 7.0),
         (5, "SA", None, 0.0),
         (6, "BA", 16, 3.0),
         (7, "UA", 12, 0.0),
         (8, None, 18, None),
         (9, "PZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    logging("-------------4、testDefaultValueFillOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    dataset_list[0].show()


def testMinMaxScalerOperator(spark):
    conf = {"input_col": "features",
            "output_col": "scaledFeatures",
            "max": 1,
            "min": 0};
    operator = MinMaxScalerOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -1.0]),),
        (1, Vectors.dense([2.0, 1.1, 1.0]),),
        (2, Vectors.dense([3.0, 10.1, 3.0]),)
    ], ["id", "features"])
    logging ("-----------------5、testMinMaxScalerOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    dataset_list[0].show()



def testStandardScalerOperator(spark):
    conf = {"input_col": "features",
            "output_col": "scaled_features",
            "with_std": True,
            "with_mean": False};
    operator = StandardScalerOperator(op_id="123", op_type="StandardScalerOperator")
    operator.conf = conf
    dataset = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -1.0]),),
        (1, Vectors.dense([2.0, 1.1, 1.0]),),
        (2, Vectors.dense([3.0, 10.1, 3.0]),)
    ], ["id", "features"])

    logging("-------------6、testStandardScalerOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    dataset_list[0].show()


def testJoinOperator(spark):
    conf = {"using_columns": "id",
            "join_type": "inner"};
    operator = JoinOperator(op_id="123", op_type="readtable")
    operator.conf = conf
    dataset1 = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    dataset2 = spark.createDataFrame(
        [(1, "lailai", 18, "female"),
         (2, "guguo", 12, "female"),
         (3, "lili", 15, "female")],
        ["id", "name", "age", "sex"])

    logging("-------------7、testJoinOperator")
    dataset1.show()
    dataset2.show()
    logging(conf)
    dataset_list = operator.handle([dataset1, dataset2], spark)
    dataset_list[0].show()


def testSplitOperator(spark):
    conf = {"weights": [0.2, 0.8],
            "seed": 123.2};
    operator = RandomSplitOperator(op_id="123", op_type="readtable")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "EA", 11, 0.0),
         (4, "CA", 12, 0.0),
         (5, "UA", 17, 0.0),
         (6, "CA", 12, 0.0),
         (7, "NZ", 18, 0.0)],
        ["id", "country", "hour", "clicked"])

    logging("-------------8、testSplitOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
      df.show()


def testMathFunctionsOperator(spark):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    conf = {"function_expressions" :["log10(hour)", "2*hour", "abs(hour)", "sqrt(hour)"]};
    operator = MathFunctionsOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    logging("-------------9、testMathFunctionsOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testBucketizerOperator(spark):
    conf = {"splits": [-float("inf"), -0.5, 0.0, 0.5, float("inf")],
            "input_col": "features",
            "output_col": "bucketedFeatures"};
    operator = BucketizerOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
    dataset = spark.createDataFrame(data, ["features"])
    logging("-------------10、testBucketizerOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()



def testFeatureExceptionSmoothOperator(spark):
    conf = {"input_col": "hour",
            "min_thresh": "7",
            "max_thresh": "15"};
    operator = FeatureExceptionSmoothOperator(op_id="123", op_type="readtable")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "CA", 5, 0.0),
         (4, "CA", 4, 0.0),
         (5, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    logging("-------------11、testFeatureExceptionSmoothOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testOneHotEncoderEstimatorOperator(spark):
    conf = {"string_indexer_input_col": "category",
            "string_indexer_output_col": "categoryIndex",
            "onehot_encoder_input_col": "categoryIndex",
            "onehot_encoder_output_col": "categoryVec",
            "drop_last": False};
    operator = OneHotEncoderEstimatorOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset = spark.createDataFrame([
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c")
    ], ["id", "category"])
    logging("-------------12、testOneHotEncoderEstimatorOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()

def testApproxQuantileOperator(spark):
    conf = {"input_col": "hour",
            "probabilities": [0.5, 0.75, 0.9, 0.95, 0.99],
            "relative_error": 0.8};
    operator = ApproxQuantileOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    logging("-------------13、testApproxQuantileOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()

def testTableStatsOperator(spark):
    conf = {"cols": ["hour", "clicked"]};
    operator = TableStatsOperator(op_id="123", op_type="readtable")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 10.0),
         (2, "CA", 12, 20.0),
         (3, "NZ", 15, 30.0),
         (4, "HK", 19, 30.0),
         (5, "MZ", 21, 30.0)],
        ["id", "country", "hour", "clicked"])
    logging("-------------14、testTableStatsOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()



def testApplyQuerySqlOperator(spark):
    logging("-------------15、testApplySqlOperator")
    conf = {"sql_query": "select * from lai_test.test1"};
    operator = ApplyQuerySqlOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset_list = operator.handle([], spark)
    dataset_list[0].show()


def testSelectOperator(spark):
    conf = {"column_name": ["country", "clicked"],
            "filter_condition": "hour>=15"};
    operator = SelectOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    logging("-------------16、testSelectOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testTableToKVOperator(spark):
    conf = {"selected_col_names": ["country", "clicked"],
            "append_col_names": ["id"]};
    operator = TableToKVOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    logging("-------------17、testTableToKVOperator")
    dataset.show()
    logging(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


if __name__ == "__main__":
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    # 1、test TableReadOperator
    testTableReadOperator(spark)
    logging('\n')
    # 2、test TableReadOperator
    testTableWriteOperator(spark)
    logging('\n')

    # 3、test TableReadOperator
    testSampleOperator(spark)
    logging('\n')

    # 4、test DefaultValueFillOperator
    testDefaultValueFillOperator(spark)
    logging('\n')

    # 5、test DefaultValueFillOperator
    testMinMaxScalerOperator(spark)
    logging('\n')

    # 6、test DefaultValueFillOperator
    testStandardScalerOperator(spark)
    logging('\n')

    # 7、test DefaultValueFillOperator
    testJoinOperator(spark)
    logging('\n')

    # 8、test DefaultValueFillOperator
    testSplitOperator(spark)
    logging('\n')

    # 9、test DefaultValueFillOperator
    testMathFunctionsOperator(spark)
    logging('\n')

    # 10、test DefaultValueFillOperator
    testBucketizerOperator(spark)
    logging('\n')

    # 11、test DefaultValueFillOperator
    testFeatureExceptionSmoothOperator(spark)
    logging('\n')

    # 12、test DefaultValueFillOperator
    testOneHotEncoderEstimatorOperator(spark)
    logging('\n')

    # 13、test DefaultValueFillOperator
    testApproxQuantileOperator(spark)
    logging('\n')

    # 14、test DefaultValueFillOperator
    testTableStatsOperator(spark)
    logging('\n')

    # 15、test DefaultValueFillOperator
    testApplyQuerySqlOperator(spark)
    logging('\n')

    # 16、test SelectOperator
    testSelectOperator(spark)
    logging('\n')

    # 17、test TableToKVOperator
    testTableToKVOperator(spark)
    logging('\n')

