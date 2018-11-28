# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

from TableReadOperator import TableReadOperator
from TableWriteOperator import TableWriteOperator
from SampleOperator import SampleOperator
from DefaultValueFillOperator import DefaultValueFillOperator
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
from NormalizedOperator import NormalizedOperator
from LabelFeatureToLibsvm import LabelFeatureToLibsvm
from VectorAssemblerOperator import VectorAssemblerOperator


def testTableReadOperator(spark):
    conf_read = {"db_name": "lai_test",
                 "table_name": "test1",
                 "partition_val": None};
    # op_id, op_type, conf, relation, result_type
    operator = TableReadOperator(op_id="123", op_type="readtable", conf=conf_read, relation="", result_type="")
    dataset_list = operator.handle([], spark)
    print("-----------------1、TableReadOperator")
    dataset_list[0].show()


def testTableWriteOperator(spark):
    conf_write = {"db_name": "lai_test",
                  "table_name": "test_save_new",
                  "save_format": None,
                  "mode": "overwrite",
                  "partition_by": "id",
                  "options": None};

    operator = TableWriteOperator(op_id="123", op_type="readtable", conf=conf_write, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "lailai", 18, "female"),
         (2, "guguo", 12, "female"),
         (3, "lili", 15, "female")],
        ["id", "name", "age", "sex"])
    print("-----------------2、testTableWriteOperator")
    dataset.show()
    operator.handle([dataset], spark)
    print("testTableWriteOperator finish")


def testSampleOperator(spark):
    conf = {"with_replacement": True,
            "fraction": "0.6",
            "seed": "325.4"};
    operator = SampleOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
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
    print("-----------------3、testSampleOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    dataset_list[0].show()


def testDefaultValueFillOperator(spark):
    conf = {"col_name_value": [["country", "china"], ["hour", "100"], ["clicked", "99.99"]]};
    operator = DefaultValueFillOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
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
    print("-------------4、testDefaultValueFillOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    dataset_list[0].show()


def testStandardScalerOperator(spark):
    conf = {"input_col": "features",
            "output_col": "scaled_features",
            "with_std": True,
            "with_mean": False};
    operator = StandardScalerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame([
        (0, Vectors.dense([1.0, 0.1, -1.0]),),
        (1, Vectors.dense([2.0, 1.1, 1.0]),),
        (2, Vectors.dense([3.0, 10.1, 3.0]),)
    ], ["id", "features"])

    print("-------------5、testStandardScalerOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    dataset_list[0].show()


def testJoinOperator(spark):
    conf = {"join_columns": [["id1", "id2"], ["country", "country"]],
            "select_left_columns": [["id1", "id1"], ["country", "country1"], ["hour1", "hour1"]],
            "select_left_columns": [["id2", "id2"], ["country", "country2"], ["hour2", "hour2"]],
            "join_type": "inner"};
    operator = JoinOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    df1 = spark.createDataFrame(
        [(1, "US", 19, 1.0),
         (2, "CA", 6, 4.0),
         (3, "CA", 20, 0.0),
         (4, "NO", 4, 7.0)],
        ["id1", "country", "hour1", "clicked1"])
    df2 = spark.createDataFrame(
        [(1, "US", 19, 1.0),
         (2, "CA", 6, 4.0),
         (3, "CA", 20, 0.0),
         (4, "NO", 4, 7.0)],
        ["id2", "country", "hour2", "clicked2"])

    print("-------------6、testJoinOperator")
    df1.show()
    df2.show()
    print(conf)
    dataset_list = operator.handle([df1, df2], spark)
    dataset_list[0].show()


def testSplitOperator(spark):
    conf = {"left_weight": "0.2",
            "right_weight": "0.8",
            "seed": 123.2};
    operator = RandomSplitOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "EA", 11, 0.0),
         (4, "CA", 12, 0.0),
         (5, "UA", 17, 0.0),
         (6, "CA", 12, 0.0),
         (7, "NZ", 18, 0.0)],
        ["id", "country", "hour", "clicked"])

    print("-------------7、testSplitOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testMathFunctionsOperator(spark):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    conf = {"scaler_conf": [
        ["hour", "log2", "True", "scaled_hour"],
        ["clicked", "sqrt", "False", "scaled_clicked"]
    ]};
    operator = MathFunctionsOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    print("-------------8、testMathFunctionsOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testBucketizerOperator(spark):
    conf = {"bucketizer_conf": [["equal_distance", "100", "features", "features_bucketed", "True"]]}
    operator = BucketizerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
    dataset = spark.createDataFrame(data, ["features"])
    print("-------------9、testBucketizerOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testFeatureExceptionSmoothOperator(spark):
    conf = {"smooth_conf": [["hour", "7", "15"], ["clicked", "10", "50"]]};
    operator = FeatureExceptionSmoothOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 10.0),
         (3, "CA", 5, 100.0),
         (4, "CA", 4, 17.0),
         (5, "NZ", 15, 8.0)],
        ["id", "country", "hour", "clicked"])
    print("-------------10、testFeatureExceptionSmoothOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testOneHotEncoderEstimatorOperator(spark):
    conf = {"input_cols": "hour,clicked",
            "output_cols": "onehot_hour,onehot_clicked",
            "drop_last": "False",
            "handle_invalid": None,
            };
    operator = OneHotEncoderEstimatorOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "CA", 5, 0.0),
         (4, "CA", 4, 0.0),
         (5, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    print("-------------11、testOneHotEncoderEstimatorOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testApproxQuantileOperator(spark):
    conf = {"input_cols": "hour, clicked",
            "probabilities": '0.5, 0.75, 0.9, 0.95, 0.99',
            "relative_error": '0.8'};
    operator = ApproxQuantileOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    print("-------------12、testApproxQuantileOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testTableStatsOperator(spark):
    conf = {"cols": ["hour", "clicked"]};
    operator = TableStatsOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 10.0),
         (2, "CA", 12, 20.0),
         (3, "NZ", 15, 30.0),
         (4, "HK", 19, 30.0),
         (5, "MZ", 21, 30.0)],
        ["id", "country", "hour", "clicked"])
    print("-------------13、testTableStatsOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testApplyQuerySqlOperator(spark):
    print("-------------14、testApplySqlOperator")
    conf = {"sql_query": "select * from lai_test.test1"};
    operator = ApplyQuerySqlOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset_list = operator.handle([], spark)
    dataset_list[0].show()


def testSelectOperator(spark):
    conf = {"column_names": ["country", "clicked"],
            "filter_condition": "hour>=15"};
    operator = SelectOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    print("-------------15、testSelectOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testTableToKVOperator(spark):
    conf = {"selected_col_names": ["country", "clicked"],
            "append_col_names": ["id"]};
    operator = TableToKVOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0),
         (2, "CA", 12, 0.0),
         (3, "NZ", 15, 0.0)],
        ["id", "country", "hour", "clicked"])
    print("-------------16、testTableToKVOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testNormalizedOperator(spark):
    conf = {"normalize_scaler_con": [["hour", "scaler_hour", "0", "1", "False"],
                                     ["clicked", "scaler_clicked", "10", "20", "False"],
                                     ["fetaure", "scaler_fetaure", "0", "1", "False"]]}
    operator = NormalizedOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, 1.0, Vectors.dense(2.7, 0.1, -1.0)),
         (2, "CA", 12, 0.0, Vectors.dense(1.6, 0.8, -1.0)),
         (3, "NZ", 15, 0.0, Vectors.dense(6.8, 6.1, -1.0))],
        ["id", "country", "hour", "clicked", "fetaure"])
    print("-------------17、testTableToKVOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testLabelFeatureToLibsvm(spark):
    conf = {"column_name": "clicked",
            "label": "id",
            "output": "label_libsvm_clicked"}
    operator = LabelFeatureToLibsvm(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, [1.0, 2.9, 4.5]),
         (2, "CA", 12, [2.0, 1.3, 7.1]),
         (3, "NZ", 15, [3.0, 2.6, 6.3])],
        ["id", "country", "hour", "clicked"])
    print("-------------18、testTableToKVOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


def testVectorAssemblerOperator(spark):
    conf = {"column_name": "clicked",
            "label": "id",
            "output": "label_libsvm_clicked"}
    operator = VectorAssemblerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
    dataset = spark.createDataFrame(
        [(1, "US", 18, [1.0, 2.9, 4.5]),
         (2, "CA", 12, [2.0, 1.3, 7.1]),
         (3, "NZ", 15, [3.0, 2.6, 6.3])],
        ["id", "country", "hour", "clicked"])
    print("-------------19、testTableToKVOperator")
    dataset.show()
    print(conf)
    dataset_list = operator.handle([dataset], spark)
    for df in dataset_list:
        df.show()


if __name__ == "__main__":
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    # 1、test TableReadOperator
    testTableReadOperator(spark)
    print('\n')
    # 2、test TableReadOperator
    testTableWriteOperator(spark)
    print('\n')

    # 3、test TableReadOperator
    testSampleOperator(spark)
    print('\n')

    # 4、test DefaultValueFillOperator
    testDefaultValueFillOperator(spark)
    print('\n')

    # 5、test DefaultValueFillOperator
    testStandardScalerOperator(spark)
    print('\n')

    # 6、test DefaultValueFillOperator
    testJoinOperator(spark)
    print('\n')

    # 7、test DefaultValueFillOperator
    testSplitOperator(spark)
    print('\n')

    # 8、test DefaultValueFillOperator
    testMathFunctionsOperator(spark)
    print('\n')

    # 9、test DefaultValueFillOperator
    testBucketizerOperator(spark)
    print('\n')

    # 10、test DefaultValueFillOperator
    testFeatureExceptionSmoothOperator(spark)
    print('\n')

    # 11、test DefaultValueFillOperator
    testOneHotEncoderEstimatorOperator(spark)
    print('\n')

    # 12、test DefaultValueFillOperator
    testApproxQuantileOperator(spark)
    print('\n')

    # 13、test DefaultValueFillOperator
    testTableStatsOperator(spark)
    print('\n')

    # 14、test DefaultValueFillOperator
    testApplyQuerySqlOperator(spark)
    print('\n')

    # 15、test SelectOperator
    testSelectOperator(spark)
    print('\n')

    # 16、test TableToKVOperator
    testTableToKVOperator(spark)
    print('\n')

    # 17、test TableToKVOperator
    testNormalizedOperator(spark)
    print('\n')

    # 18、test LabelFeatureToLibsvm
    testLabelFeatureToLibsvm(spark)
    print('\n')
