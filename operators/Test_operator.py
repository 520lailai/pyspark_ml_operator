# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

from TableReadOperator import TableReadOperator
from TableWriteOperator import TableWriteOperator
from SampleOperator import SampleOperator
from DefaultValueFillOperator import DefaultValueFillOperator
from MinMaxScalerOperator import MinMaxScalerOperator
from StandardScalerOperator import StandardScalerOperator
from JoinOperator import JoinOperator
from SplitOperator import SplitOperator
from BucketizerOperator import BucketizerOperator
from FeatureExceptionSmoothOperator import FeatureExceptionSmoothOperator
from OneHotEncoderEstimatorOperator import OneHotEncoderEstimatorOperator
from ApplySqlOperator import ApplySqlOperator
from MathFunctionsOperator import MathFunctionsOperator
from ApproxQuantileOperator import ApproxQuantileOperator
from TableStatsOperator import TableStatsOperator


def testTableReadOperator(spark):
    conf_read = {"db_name": "lai_test",
                 "table_name": "test1",
                 "partition_val": None};
    operator = TableReadOperator(op_id="123", op_type="readtable")
    operator.conf = conf_read
    datafrme = operator.handle([], spark)
    print ("-----------------1、TableReadOperator")
    datafrme[0].show()


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
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    operator.handle([dataset], spark)
    print ("-----------------2、testTableWriteOperator")


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
    dataset = operator.handle([dataset], spark)
    print("-------------3、testSampleOperator success")
    dataset[0].show()


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
    dataset = operator.handle([dataset], spark)
    print("-------------4、testDefaultValueFillOperator success")
    dataset[0].show()


def testMinMaxScalerOperator(spark):
    conf = {"input_col": "features",
            "output_col": "scaledFeatures",
            "max": 1,
            "min": 0};
    operator = MinMaxScalerOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataFrame = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -1.0]),),
        (1, Vectors.dense([2.0, 1.1, 1.0]),),
        (2, Vectors.dense([3.0, 10.1, 3.0]),)
    ], ["id", "features"])
    dataset = operator.handle([dataFrame], spark)
    print("-------------5、testMinMaxScalerOperator success")
    dataset[0].show()


def testStandardScalerOperator(spark):
    conf = {"input_col": "features",
            "output_col": "scaled_features",
            "with_std": True,
            "with_mean": False};
    operator = StandardScalerOperator(op_id="123", op_type="StandardScalerOperator")
    operator.conf = conf
    dataFrame = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -1.0]),),
        (1, Vectors.dense([2.0, 1.1, 1.0]),),
        (2, Vectors.dense([3.0, 10.1, 3.0]),)
    ], ["id", "features"])

    dataset = operator.handle([dataFrame], spark)
    print("-------------6、testStandardScalerOperator success")
    dataset[0].show()


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

    dataset = operator.handle([dataset1, dataset2], spark)
    print("-------------7、testStandardScalerOperator success")
    dataset[0].show()


def testSplitOperator(spark):
    conf = {"weights": [0.2, 0.8],
            "seed": 123.2};
    operator = SplitOperator(op_id="123", op_type="readtable")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    dataset = operator.handle([dataset], spark)
    print("-------------8、testSplitOperator success")
    dataset[0].show()


def testMathFunctionsOperator(spark):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    conf = {"input_col": "hour",
            "function_name": "log10"};
    operator = MathFunctionsOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    dataset = operator.handle([dataset], spark)
    print("-------------9、testMathFunctionsOperator success")
    dataset[0].show()


def testBucketizerOperator(spark):
    conf = {"splits": [-float("inf"), -0.5, 0.0, 0.5, float("inf")],
            "input_col": "features",
            "output_col": "bucketedFeatures"};
    operator = BucketizerOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
    dataFrame = spark.createDataFrame(data, ["features"])
    dataset = operator.handle([dataFrame], spark)
    print("-------------10、testBucketizerOperator success")
    dataset[0].show()


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
    dataset = operator.handle([dataset], spark)
    print("-------------11、testFeatureExceptionSmoothOperator success")
    dataset[0].show()


def testOneHotEncoderEstimatorOperator(spark):
    conf = {"string_indexer_input_col": "category",
            "string_indexer_output_col": "categoryIndex",
            "onehot_encoder_input_col": "categoryIndex",
            "onehot_encoder_output_col": "categoryVec",
            "drop_last": False};
    operator = OneHotEncoderEstimatorOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    df = spark.createDataFrame([
        (0, "a"),
        (1, "b"),
        (2, "c"),
        (3, "a"),
        (4, "a"),
        (5, "c")
    ], ["id", "category"])
    dataset = operator.handle([df], spark)
    print("-------------12、testOneHotEncoderEstimatorOperator success")
    dataset[0].show()


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
    dataset = operator.handle([dataset], spark)
    print("-------------13、testApproxQuantileOperator success")
    dataset[0].show()


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
    operator.handle([dataset], spark)
    print("-------------14、testDescribeOperator success")
    dataset[0].show()


def testApplySqlOperator(spark):
    conf = {"sql": "select * from lai_test.test1"};
    operator = ApplySqlOperator(op_id="123", op_type="SelectOperator")
    operator.conf = conf
    dataset = operator.handle([], spark)
    print("-------------15、testApplySqlOperator success")
    dataset[0].show()


if __name__ == "__main__":
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    # 1、test TableReadOperator
    testTableReadOperator(spark)

    # 2、test TableReadOperator
    testTableWriteOperator(spark)

    # 3、test TableReadOperator
    testSampleOperator(spark)

    # 4、test DefaultValueFillOperator
    testDefaultValueFillOperator(spark)

    # 5、test DefaultValueFillOperator
    testMinMaxScalerOperator(spark)

    # 6、test DefaultValueFillOperator
    testStandardScalerOperator(spark)

    # 7、test DefaultValueFillOperator
    testJoinOperator(spark)

    # 8、test DefaultValueFillOperator
    testSplitOperator(spark)

    # 9、test DefaultValueFillOperator
    testMathFunctionsOperator(spark)

    # 10、test DefaultValueFillOperator
    testBucketizerOperator(spark)

    # 11、test DefaultValueFillOperator
    testFeatureExceptionSmoothOperator(spark)

    # 12、test DefaultValueFillOperator
    testOneHotEncoderEstimatorOperator(spark)

    # 13、test DefaultValueFillOperator
    testApproxQuantileOperator(spark)

    # 14、test DefaultValueFillOperator
    testTableStatsOperator(spark)

    # 15、test DefaultValueFillOperator
    testApplySqlOperator(spark)

