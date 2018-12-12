# -*- coding: utf-8 -*-
import unittest
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import json
from RedisUtils import RedisUtils
from pyspark.sql import Row

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
from NormalizedOperator import NormalizedOperator
from LabelFeatureToLibsvmOperator import *
from VectorAssemblerOperator import VectorAssemblerOperator
from WriteRedisOperator import WriteRedisOperator
from JsonUtils import ExtendJSONEncoder


class UnitTestOperators(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    def test_tableReadOperator(self):
        conf_read1 = {"db_name": "lai_test",
                      "table_name": "test1",
                      "limit": "2",
                      "partition_val": None};
        operator = TableReadOperator(op_id="123", op_type="readtable", conf=conf_read1, relation="", result_type="")
        # 1、测试读取的结果的正确性
        dataset_list = operator.handle([], self.spark)
        case = self.spark.createDataFrame(
            [(7, "US", 18, 1.0),
             (8, "CA", 12, 0.0),
             (9, "NZ", 15, 0.0),
             (1, "US", 12, 1.0),
             (5, "US", 12, 1.0),
             (3, "US", 12, 1.0),
             (2, "US", 12, 1.0),
             (6, "US", 12, 1.0),
             (4, "US", 12, 1.0)],
            ["id", "country", "hour", "clicked"])

        self.assertEqual(case.sort(["id"]).collect(), dataset_list[0].sort(["id"]).collect())

        conf_read2 = {"db_name": None,
                      "table_name": "test1",
                      "limit": "2",
                      "partition_val": None};

        operator = TableReadOperator(op_id="123", op_type="readtable", conf=conf_read2, relation="", result_type="")

        # 2、测试读取的过程抛出异常
        with self.assertRaises(ParameterException):
            operator.handle([], self.spark)

    def test_tableWriteOperator(self):
        dataset = self.spark.createDataFrame(
            [(1, "lailai", 18, "female"),
             (2, "guguo", 12, "female"),
             (3, "lili", 15, "female")],
            ["id", "name", "age", "sex"])

        # 1、测试写入数据的结果正确性
        conf_write1 = {"db_name": "lai_test",
                       "table_name": "test_save_new",
                       "partition_by": "id",
                       "limit_num": 100};
        operator = TableWriteOperator(op_id="123", op_type="readtable", conf=conf_write1, relation="", result_type="")
        dataframe_list = operator.handle([dataset], self.spark)
        self.assertEqual(dataset.sort(["id"]).collect(), dataframe_list[0].sort(["id"]).collect())

        # 2、测试读取的过程抛出异常
        conf_write2 = {"db_name": "lala",
                       "table_name": "test_save_new",
                       "partition_by": "id",
                       "limit_num": 100};
        operator = TableWriteOperator(op_id="123", op_type="readtable", conf=conf_write2, relation="", result_type="")
        with self.assertRaises(Exception):
            dataframe_list = operator.handle([dataset], self.spark)

    def test_sampleOperator(self):
        conf = {"with_replacement": False,
                "fraction": "0.6",
                "seed": None};
        operator = SampleOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset = self.spark.createDataFrame(
            [(1, "US", 18, 1.0),
             (2, "CA", 12, 5.0),
             (3, "CA", 12, 0.0),
             (4, "CA", 14, 7.0),
             (5, "SA", 12, 0.0),
             (6, "BA", 16, 3.0),
             (7, "CA", 14, 7.0),
             (8, "SA", 12, 0.0),
             (9, "BA", 16, 3.0),
             (10, "CA", 14, 7.0),
             (11, "SA", 12, 0.0),
             (12, "BA", 16, 3.0),
             (13, "CA", 14, 7.0),
             (14, "SA", 12, 0.0),
             (15, "BA", 16, 3.0),
             (16, "UA", 12, 0.0),
             (17, "OA", 18, 5.0),
             (18, "UA", 12, 0.0),
             (19, "OA", 18, 5.0),
             (20, "PZ", 15, 0.0)],
            ["id", "country", "hour", "clicked"])

        count = 0.0;
        # 1、测试数据的采样的比例是否合理
        for i in range(1, 100):
            dataset_list = operator.handle([dataset], self.spark)
            count += dataset_list[0].count()

        self.assertEqual(round(count / (1000 * 20), 1), round(0.6, 1))

    def test_defaultValueFillOperator(self):
        conf = {"col_name_value": [["country", "china"], ["hour", "100"], ["clicked", "99.99"]]};
        operator = DefaultValueFillOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset = self.spark.createDataFrame(
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
        dataset_list = operator.handle([dataset], self.spark)

        dataset_re = self.spark.createDataFrame(
            [(1, "US", 18, 1.0),
             (2, "CA", 12, 99.99),
             (3, "CA", 12, 0.0),
             (4, "CA", 14, 7.0),
             (5, "SA", 100, 0.0),
             (6, "BA", 16, 3.0),
             (7, "UA", 12, 0.0),
             (8, "china", 18, 99.99),
             (9, "PZ", 15, 0.0)],
            ["id", "country", "hour", "clicked"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())

    def test_standardScalerOperator(self):
        conf = {"standard_scaler_conf": [["features", "scaled_features", "True", "False", "False"]]};
        operator = StandardScalerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset = self.spark.createDataFrame([
            (0, Vectors.dense(1.0, 0.1, -1.0),),
            (1, Vectors.dense(2.0, 1.1, 1.0),),
            (2, Vectors.dense(3.0, 10.1, 3.0),)
        ], ["id", "features"])

        dataset_list = operator.handle([dataset], self.spark)
        dataset_re = self.spark.createDataFrame([
            (0, Vectors.dense(1.0, 0.1, -1.0), Vectors.dense(1.0, 0.018156825980064073, -0.50)),
            (1, Vectors.dense(2.0, 1.1, 1.0), Vectors.dense(2.0, 0.19972508578070483, 0.5)),
            (2, Vectors.dense(3.0, 10.1, 3.0), Vectors.dense(3.0, 1.8338394239864713, 1.5))
        ], ["id", "features", "scaled_features"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())

    def test_joinOperator(self):
        conf = {'join_columns': [['id1', 'id2'], ['country', 'country']],
                'select_right_columns': [['id2', 'id2'], ['country', 'country2'], ['hour2', 'hour2']],
                'join_type': 'inner',
                'select_left_columns': [['id1', 'id1'], ['country', 'country1'], ['hour1', 'hour1']]};
        operator = JoinOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        df1 = self.spark.createDataFrame(
            [(1, "US", 19, 1.0),
             (2, "CA", 6, 4.0),
             (3, "CA", 20, 0.0),
             (4, "NO", 4, 7.0)],
            ["id1", "country", "hour1", "clicked1"])

        df2 = self.spark.createDataFrame(
            [(1, "US", 19, 1.0),
             (2, "CA", 6, 4.0),
             (3, "CA", 20, 0.0),
             (4, "NO", 4, 7.0)],
            ["id2", "country", "hour2", "clicked2"])

        dataset_list = operator.handle([df1, df2], self.spark)

        dataset_re = self.spark.createDataFrame(
            [(2, "CA", 6, 2, "CA", 6),
             (3, "CA", 20, 3, "CA", 20),
             (1, "US", 19, 1, "US", 19),
             (4, "NO", 4, 4, "NO", 4)],
            ["id1", "country1", "hour1", "id2", "country2", "hour2"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["id1"]).collect(), dataset_re.sort(["id1"]).collect())

    def test_splitOperator(self):
        conf = {"left_weight": "0.2",
                "right_weight": "0.8",
                "seed": None};
        operator = RandomSplitOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset = self.spark.createDataFrame(
            [(1, "US", 18, 1.0),
             (2, "CA", 12, 0.0),
             (3, "EA", 11, 0.0),
             (4, "CA", 12, 0.0),
             (5, "UA", 17, 0.0),
             (6, "EA", 11, 0.0),
             (7, "CA", 12, 0.0),
             (8, "UA", 17, 0.0),
             (9, "EA", 11, 0.0),
             (10, "CA", 12, 0.0),
             (11, "UA", 17, 0.0),
             (12, "EA", 11, 0.0),
             (13, "CA", 12, 0.0),
             (14, "UA", 17, 0.0),
             (15, "CA", 12, 0.0),
             (16, "NZ", 18, 0.0),
             (17, "NZ", 18, 0.0),
             (18, "NZ", 18, 0.0),
             (19, "NZ", 18, 0.0),
             (20, "NZ", 18, 0.0)],
            ["id", "country", "hour", "clicked"])

        left_count = 0.0;
        right_count = 0.0;
        for i in range(1, 100):
            dataset_list = operator.handle([dataset], self.spark)
            left_count += dataset_list[0].count()
            right_count += dataset_list[1].count()

        # 1、拆分的左右量表的比例：
        try:
            weight = float(left_count) / float(right_count)
        except ZeroDivisionError:
            weight = 0.0
        self.assertEqual(round(weight, 1), round(0.2 / 0.8, 1))

    def test_mathFunctionsOperator(self):
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

        dataset_list = operator.handle([dataset], spark)

        dataset_re = self.spark.createDataFrame(
            [(1, "US", 4.169925001442312, 1.0, 1.0),
             (2, "CA", 3.5849625007211565, 0.0, 0.0),
             (3, "NZ", 3.9068905956085187, 0.0, 0.0)],
            ["id", "country", "scaled_hour", "clicked", "scaled_clicked"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())

    def test_bucketizerOperator(self):
        conf = {"bucketizer_conf": [["isometric_discretization", "100", "features", "features_bucketed", "True"]]}
        operator = BucketizerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
        dataset = self.spark.createDataFrame(data, ["features"])
        dataset_list = operator.handle([dataset], self.spark)

        dataset_re = self.spark.createDataFrame(
            [(-999.9, 0.0),
             (-0.5, 9.0),
             (-0.3, 9.0),
             (0.0, 9.0),
             (0.2, 10.0),
             (999.9, 19.0)],
            ["features", "features_bucketed"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["features"]).collect(), dataset_re.sort(["features"]).collect())

    def test_featureExceptionSmoothOperator(self):
        conf = {"smooth_conf": [["hour", "7", "15"], ["clicked", "10", "50"]]};
        operator = FeatureExceptionSmoothOperator(op_id="123", op_type="readtable", conf=conf, relation="",
                                                  result_type="")
        dataset = self.spark.createDataFrame(
            [(1, "US", 18, 1.0),
             (2, "CA", 12, 10.0),
             (3, "CA", 5, 100.0),
             (4, "CA", 4, 17.0),
             (5, "NZ", 15, 8.0)],
            ["id", "country", "hour", "clicked"])

        dataset_re = self.spark.createDataFrame(
            [(1, "US", 15, float(10)),
             (2, "CA", 12, float(10)),
             (3, "CA", 7, 50.0),
             (4, "CA", 7, 17.0),
             (5, "NZ", 15, 10.0)],
            ["id", "country", "hour", "clicked"])

        dataset_list = operator.handle([dataset], self.spark)
        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())

    def test_oneHotEncoderEstimatorOperator(self):
        conf = {"onehot_conf": [["country", "country_onehot"], ["hour", "hour-onehot"], ["score", "score-onehot"]],
                "drop_last": True,
                "handle_invalid": "keep",
                "other_col_output": ["id", "clicked"],
                "is_output_model": True,
                };
        operator = OneHotEncoderEstimatorOperator(op_id="123", op_type="readtable", conf=conf, relation="",
                                                  result_type="")

        dataset = self.spark.createDataFrame(
            [(1, "China", 18, 1.5, 2),
             (2, "America", 12, 0.0, 4),
             (3, "Brazil", 5, 0.5, 5),
             (4, "united kiongdom", 4, 6.7, 9),
             (5, "Vietnam", 15, 0.0, 5)],
            ["id", "country", "hour", "score", "clicked"])

        dataset_re = self.spark.createDataFrame(
            [(1, 2, Vectors.sparse(6, [0], [1.0]), Vectors.sparse(19, [18], [1.0]), Vectors.sparse(5, [2], [1.0])),
             (2, 4, Vectors.sparse(6, [4], [1.0]), Vectors.sparse(19, [12], [1.0]), Vectors.sparse(5, [0], [1.0])),
             (3, 5, Vectors.sparse(6, [2], [1.0]), Vectors.sparse(19, [5], [1.0]),  Vectors.sparse(5, [1], [1.0])),
             (4, 9, Vectors.sparse(6, [1], [1.0]), Vectors.sparse(19, [4], [1.0]),  Vectors.sparse(5, [3], [1.0])),
             (5, 5, Vectors.sparse(6, [3], [1.0]), Vectors.sparse(19, [15], [1.0]), Vectors.sparse(5, [0], [1.0]))],
            ["id", "clicked", "country_onehot", "hour-onehot", "score-onehot"])

        mapping_re = self.spark.createDataFrame(
            [("country", "united kiongdom", 1.0),
             ("country", "China", 0.0),
             ("country", "Vietnam", 3.0),
             ("country", "Brazil", 2.0),
             ("country", "America", 4.0),
             ("score", "6.7", 3.0),
             ("score", "0.5", 1.0),
             ("score", "1.5", 2.0),
             ("score", "0.0", 0.0)],
            ["col_name", "col_value", "mapping"])

        # 1、测试结果的正确性
        dataset_list = operator.handle([dataset], self.spark)
        self.assertEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())
        self.assertEqual(dataset_list[1].sort(["col_name", "col_value"]).collect(),
                         mapping_re.sort(["col_name", "col_value"]).collect())

        return dataset_list

    def test_oneHotEncoderEstimatorOperator2(self):
        conf = {"onehot_conf": [["country","country_onehot"],["hour", "hour-onehot"],["score","score-onehot"]],
                "drop_last": True,
                "handle_invalid": "keep",
                "other_col_output": ["id", "clicked"],
                "is_output_model": True,
                };
        
        operator = OneHotEncoderEstimatorOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")

        dataset = self.spark.createDataFrame(
            [(1, "China", 18, 1.5, 2),
             (2, "America", 12, 0.0, 4),
             (3, "Brazil", 5, 0.5, 5),
             (4, "united kiongdom ", 4, 6.7, 9),
             (5, "Vietnam", 15, 0.0, 5)],
            ["id", "country", "hour", "score", "clicked"])

        modle = self.spark.createDataFrame(
            [("country", "united kiongdom", 1.0),
             ("country", "China", 0.0),
             ("country", "Vietnam", 3.0),
             ("country", "Brazil", 2.0),
             ("country", "America", 4.0),
             ("score", "6.7", 3.0),
             ("score", "0.5", 1.0),
             ("score", "1.5", 2.0),
             ("score", "0.0", 0.0)],
            ["col_name", "col_value", "mapping"])

        dataset_list = operator.handle([dataset, modle], self.spark)

        dataset_re = self.spark.createDataFrame(
            [(1, 2, Vectors.sparse(6, [0], [1.0]), Vectors.sparse(19, [18], [1.0]), Vectors.sparse(5, [2], [1.0])),
             (2, 4, Vectors.sparse(6, [4], [1.0]), Vectors.sparse(19, [12], [1.0]), Vectors.sparse(5, [0], [1.0])),
             (3, 5, Vectors.sparse(6, [2], [1.0]), Vectors.sparse(19, [5], [1.0]),  Vectors.sparse(5, [1], [1.0])),
             (4, 9, Vectors.sparse(6, [1], [1.0]), Vectors.sparse(19, [4], [1.0]),  Vectors.sparse(5, [3], [1.0])),
             (5, 5, Vectors.sparse(6, [3], [1.0]), Vectors.sparse(19, [15], [1.0]), Vectors.sparse(5, [0], [1.0]))],
            ["id", "clicked", "country_onehot", "hour-onehot", "score-onehot"])

        # 1、测试结果的正确性
        self.assertNotEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())
        self.assertNotEqual(dataset_list[1].sort(["col_name", "col_value"]).collect(),
                            modle.sort(["col_name", "col_value"]).collect())

    def test_approxQuantileOperator(self):
        conf = {"input_cols": "hour, clicked",
                "probabilities": '0.5, 0.75, 0.9, 0.95, 0.99',
                "relative_error": '0.8'};
        operator = ApproxQuantileOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset = self.spark.createDataFrame(
            [(1, "US", 18, 1.0),
             (2, "CA", 12, 0.0),
             (3, "NZ", 15, 0.0)],
            ["id", "country", "hour", "clicked"])

        dataset_list = operator.handle([dataset], self.spark)

        dataset_re = self.spark.createDataFrame(
            [("hour", 12.0, 12.0, 18.0, 18.0, 18.0),
             ("clicked", 0.0, 0.0, 1.0, 1.0, 1.0)],
            ["colum_name", "p0.5", "p0.75", "p0.9", "p0.95", "p0.99"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].collect(), dataset_re.collect())

    def test_tableStatsOperator(self):
        conf = {"cols": ["hour", "clicked"]};
        operator = TableStatsOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")

        dataset = self.spark.createDataFrame(
            [(1, "US", 18, 10.0),
             (2, "CA", 12, 20.0),
             (3, "NZ", 15, 30.0),
             (4, "HK", 19, 30.0),
             (5, "MZ", 21, 30.0)],
            ["id", "country", "hour", "clicked"])

        dataset_re = self.spark.createDataFrame(
            [("count", "5", "5"),
             ("mean", "17.0", "24.0"),
             ("stddev", "3.5355339059327378", "8.94427190999916"),
             ("min", "12", "10.0"),
             ("25%", "15", "20.0"),
             ("50%", "18", "30.0"),
             ("75%", "19", "30.0"),
             ("max", "21", "30.0")],
            ["summary", "hour", "clicked"])

        dataset_list = operator.handle([dataset], self.spark)
        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["summary"]).collect(), dataset_re.sort(["summary"]).collect())

    def test_applyQuerySqlOperator(self):
        print("-------------14、testApplySqlOperator")
        conf = {"sql_query": "select * from lai_test.test1"};
        operator = ApplyQuerySqlOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset_list = operator.handle([], self.spark)
        dataset_list[0].show(truncate=False)

        dataset_re = self.spark.createDataFrame(
            [(7, "US", 18, 1.0),
             (8, "CA", 12, 0.0),
             (9, "NZ", 15, 0.0),
             (1, "US", 12, 1.0),
             (5, "US", 12, 1.0),
             (3, "US", 12, 1.0),
             (2, "US", 12, 1.0),
             (6, "US", 12, 1.0),
             (4, "US", 12, 1.0)],
            ["id", "country", "hour", "clicked"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())

    def test_selectOperator(self):
        conf = {"column_names": ["country", "clicked"],
                "filter_condition": "hour>=15"};
        operator = SelectOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset = self.spark.createDataFrame(
            [(1, "US", 18, 1.0),
             (2, "CA", 12, 0.0),
             (3, "NZ", 15, 0.0)],
            ["id", "country", "hour", "clicked"])

        dataset_list = operator.handle([dataset], self.spark)
        dataset_re = self.spark.createDataFrame(
            [("US", 1.0),
             ("NZ", 0.0)],
            ["country", "clicked"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["country"]).collect(), dataset_re.sort(["country"]).collect())

    def test_normalizedOperator(self):
        conf = {"normalize_scaler_conf": [["hour", "scaler_hour", "0", "1", "False"],
                                          ["clicked", "scaler_clicked", "10", "20", "False"],
                                          ["fetaure", "scaler_fetaure", "0", "1", "False"]]}
        operator = NormalizedOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")

        dataset = self.spark.createDataFrame(
            [(1, "US", 18, 1.0, Vectors.dense(2.7, 0.1, -1.0)),
             (2, "CA", 12, 0.0, Vectors.dense(1.6, 0.8, -1.0)),
             (3, "NZ", 15, 0.0, Vectors.dense(6.8, 6.1, -1.0))],
            ["id", "country", "hour", "clicked", "fetaure"])

        dataset_list = operator.handle([dataset], self.spark)

        dataset_re = self.spark.createDataFrame(
            [(1, "US", 18, 1.0, Vectors.dense(2.7, 0.1, -1.0), 1.0, 1.0, Vectors.dense(0.2115384615384616, 0.0, 0.5)),
             (2, "CA", 12, 0.0, Vectors.dense(1.6, 0.8, -1.0), 0.0, 0.0, Vectors.dense(0.0, 0.11666666666666668, 0.5)),
             (3, "NZ", 15, 0.0, Vectors.dense(6.8, 6.1, -1.0), 0.5, 0.0, Vectors.dense(1.0, 1.0, 0.5))],
            ["id", "country", "hour", "clicked", "fetaure", "scaler_hour", "scaler_clicked", "scaler_fetaure"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())

    def test_labelFeatureToLibsvm(self):
        conf = {"column_name": "clicked",
                "label": "id",
                "output": "label_libsvm_clicked"}
        operator = LabelFeatureToLibsvmOperator(op_id="123", op_type="readtable", conf=conf, relation="",
                                                result_type="")
        dataset = self.spark.createDataFrame(
            [(1, "US", 18, Vectors.dense(1.0, 2.9, 4.5)),
             (2, "CA", 12, Vectors.dense(2.0, 1.3, 7.1)),
             (3, "NZ", 15, Vectors.dense(3.0, 2.6, 6.3))],
            ["id", "country", "hour", "clicked"])

        dataset_list = operator.handle([dataset], self.spark)

        dataset_re = self.spark.createDataFrame(
            [Row("1.0 1:1.0 2:2.9 3:4.5"),
             Row("2.0 1:2.0 2:1.3 3:7.1"),
             Row("3.0 1:3.0 2:2.6 3:6.3")],
            ["label_libsvm_clicked"])

        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["label_libsvm_clicked"]).collect(),
                         dataset_re.sort(["label_libsvm_clicked"]).collect())

    def test_VectorAssemblerOperator(self):
        conf = {"column_names": ["hour", "clicked"],
                "output": "label_libsvm_clicked"}
        operator = VectorAssemblerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset = self.spark.createDataFrame(
            [(1, "US", 18, Vectors.dense(1.0, 2.9, 4.5)),
             (2, "CA", 12, Vectors.dense(2.0, 1.3, 7.1)),
             (3, "NZ", 15, Vectors.dense(3.0, 2.6, 6.3))],
            ["id", "country", "hour", "clicked"])
        dataset_list = operator.handle([dataset], self.spark)
        dataset_re = self.spark.createDataFrame(
            [(1, "US", 18, Vectors.dense(1.0, 2.9, 4.5), Vectors.dense(18.0, 1.0, 2.9, 4.5)),
             (2, "CA", 12, Vectors.dense(2.0, 1.3, 7.1), Vectors.dense(12.0, 2.0, 1.3, 7.1)),
             (3, "NZ", 15, Vectors.dense(3.0, 2.6, 6.3), Vectors.dense(15.0, 3.0, 2.6, 6.3))],
            ["id", "country", "hour", "clicked", "label_libsvm_clicked"])
        # 1、测试结果的正确性
        self.assertEqual(dataset_list[0].sort(["id"]).collect(), dataset_re.sort(["id"]).collect())

    def test_writeRedisOperator(self):
        conf = {"key": ""}
        operator = WriteRedisOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset = self.spark.createDataFrame(
            [(1, "US", 18, Vectors.dense(1.0, 2.9, 4.5)),
             (2, "CA", 12, Vectors.dense(2.0, 1.3, 7.1)),
             (3, "NZ", 15, Vectors.dense(3.0, 2.6, 6.3))],
            ["id", "country", "hour", "clicked"])

        # 1、测试读取的过程抛出异常
        with self.assertRaises(ParameterException):
            operator.handle([dataset], self.spark)

        # 2、测试写数据
        conf = {"key": "test_redis_key"}
        operator = WriteRedisOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        operator.handle([dataset], self.spark)

        keyorder = dataset.columns
        rows = []
        for row in dataset.collect():
            row_dict = row.asDict(True)
            sorted(row_dict.items(), key=lambda i: keyorder.index(i[0]))
            rows.append(row_dict)
        data = json.dumps(rows, cls=ExtendJSONEncoder)

        data_r = RedisUtils.read_redis("test_redis_key")

        self.assertEqual(data, data_r)


if __name__ == "__main__":
    unittest.main()
