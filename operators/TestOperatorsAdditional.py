# -*- coding: utf-8 -*-
import sys
import os

o_path = os.getcwd()
sys.path.append(o_path)
sys.path.append("..")

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row

from OneHotEncoderEstimatorOperator import OneHotEncoderEstimatorOperator
from SampleOperator import SampleOperator
from LabelFeatureToLibsvmOperator import LabelFeatureToLibsvmOperator
from RandomSplitOperator import RandomSplitOperator
from TableStatsOperator import TableStatsOperator
from BucketizerOperator import BucketizerOperator


class TestOperatorsAdditional:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    def test_oneHotEncoderEstimatorOperator(self):
        print("------------test_oneHotEncoderEstimatorOperator:--------------")
        conf = {"onehot_conf": [["country", "country_onehot"], ["hour", "hour-onehot"], ["score", "score-onehot"]],
                "drop_last": True,
                "handle_invalid": "error",
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

        print("----------input_table")
        dataset.show()

        dataset_re = self.spark.createDataFrame(
            [(1, 2, Vectors.sparse(5,[2],[1.0]), Vectors.sparse(5,[2],[1.0]), Vectors.sparse(4,[1],[1.0])),
             (2, 4, Vectors.sparse(5,[4],[1.0]), Vectors.sparse(5,[4],[1.0]), Vectors.sparse(4,[3],[1.0])),
             (3, 5, Vectors.sparse(5,[3],[1.0]), Vectors.sparse(5,[0],[1.0]), Vectors.sparse(4,[2],[1.0])),
             (4, 9, Vectors.sparse(5,[0],[1.0]), Vectors.sparse(5,[1],[1.0]), Vectors.sparse(4,[0],[1.0])),
             (5, 5, Vectors.sparse(5,[1],[1.0]), Vectors.sparse(5,[3],[1.0]), Vectors.sparse(4,[3],[1.0]))],
            ["id", "clicked", "country_onehot", "hour-onehot", "score-onehot"])

        mapping_re = self.spark.createDataFrame(
            [("country", "Vietnam", 1.0),
             ("country", "Brazil", 3.0),
             ("country", "China", 2.0),
             ("country", "America", 4.0),
             ("country", "united kiongdom", 0.0),
             ("hour", "12", 4.0),
             ("hour", "15", 3.0),
             ("hour", "18", 2.0),
             ("hour", "4", 1.0),
             ("hour", "5", 0.0),
             ("score", "6.7", 0.0),
             ("score", "1.5", 1.0),
             ("score", "0.5", 2.0),
             ("score", "0.0", 3.0)],
            ["col_name", "col_value", "mapping"])

        print("----------my_predict_result_table")
        dataset_re.show()
        print("----------my_predict_result_modle")
        mapping_re.show()

        # 1、测试结果的正确性
        dataset_list = operator.handle([dataset], self.spark)

        print("----------result_table")
        dataset_list[0].show()
        print("----------result_modle")
        dataset_list[1].show()

        return dataset_list

    def test_oneHotEncoderEstimatorOperator2(self):
        print("------------test_oneHotEncoderEstimatorOperator2:--------------")
        conf = {"onehot_conf": [["country", "country_onehot"], ["hour", "hour-onehot"], ["score", "score-onehot"]],
                "drop_last": True,
                "handle_invalid": "error",
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
            [(1, 2, Vectors.sparse(5,[2],[1.0]), Vectors.sparse(5,[2],[1.0]), Vectors.sparse(4,[1],[1.0])),
             (2, 4, Vectors.sparse(5,[4],[1.0]), Vectors.sparse(5,[4],[1.0]), Vectors.sparse(4,[3],[1.0])),
             (3, 5, Vectors.sparse(5,[3],[1.0]), Vectors.sparse(5,[0],[1.0]), Vectors.sparse(4,[2],[1.0])),
             (4, 9, Vectors.sparse(5,[0],[1.0]), Vectors.sparse(5,[1],[1.0]), Vectors.sparse(4,[0],[1.0])),
             (5, 5, Vectors.sparse(5,[1],[1.0]), Vectors.sparse(5,[3],[1.0]), Vectors.sparse(4,[3],[1.0]))],
            ["id", "clicked", "country_onehot", "hour-onehot", "score-onehot"])

        modle = self.spark.createDataFrame(
            [("country", "Vietnam", 1.0),
             ("country", "Brazil", 3.0),
             ("country", "China", 2.0),
             ("country", "America", 4.0),
             ("country", "united kiongdom", 0.0),
             ("hour", "12", 4.0),
             ("hour", "15", 3.0),
             ("hour", "18", 2.0),
             ("hour", "4", 1.0),
             ("hour", "5", 0.0),
             ("score", "6.7", 0.0),
             ("score", "1.5", 1.0),
             ("score", "0.5", 2.0),
             ("score", "0.0", 3.0)],
            ["col_name", "col_value", "mapping"])

        print("---------input-table------")
        dataset.show()

        print("---------input-modle------")
        modle.show()

        dataset_list = operator.handle([dataset, modle], self.spark)

        print("---------result-table------")
        dataset_list[0].show()
        print("---------result-modle------")
        dataset_list[1].show()


        print("---------my_predict_result-table------")
        dataset_re.show()

        print("---------my_predict_result-model------")
        modle.show()

    def test_sampleOperator(self):
        print("---------test_sampleOperator------")
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
        dataset.show()
        print(conf)
        dataset_list = operator.handle([dataset], self.spark)
        dataset_list[0].show()

    def test_splitOperator(self):
        print("------------test_splitOperator:--------------")
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

        dataset_list = operator.handle([dataset], self.spark)

        dataset.show()
        print(conf)

        dataset_list[0].show()
        dataset_list[1].show()

    def test_labelFeatureToLibsvm(self):
        print("-------------test_labelFeatureToLibsvm--")
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

        dataset.show()
        print(conf)
        dataset_list[0].show()

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

        dataset.show()
        print(conf)
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
        print("-------- result_table -----")
        dataset_list[0].show()

        print("-------- dataset_re -----")
        dataset_re.show()

    def test_bucketizerOperator(self):
        print("------------test_bucketizerOperator--------")
        data = [(-999.9,), (-0.5,), (-0.3,), (0.0,), (0.2,), (999.9,)]
        dataset = self.spark.createDataFrame(data, ["features"])

        print("------------ dataset--------")
        dataset.show()

        # 1、测试等距离散
        conf = {"bucketizer_conf": [["isometric_discretization", "100", "features", "features_bucketed", "False"]]}
        operator = BucketizerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset_list = operator.handle([dataset], self.spark)
        dataset_re = self.spark.createDataFrame(
            [(-999.9, 0.0),
             (-0.5, 9.0),
             (-0.3, 9.0),
             (0.0, 9.0),
             (0.2, 10.0),
             (999.9, 19.0)],
            ["features", "features_bucketed"])

        print("------------ predict : dataset_re--------")
        dataset_re.show()

        print("------------等距离散 result:--------")
        print(conf)
        dataset_list[0].show()

        # 2、测试等频离散
        conf = {"bucketizer_conf": [["isofrequecy_discretization", "2", "features", "features_bucketed", "False"]]}
        operator = BucketizerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset_list = operator.handle([dataset], self.spark)
        print("------------等频离散  result:--------")
        print(conf)
        dataset_list[0].show()

        # 3、自定义离散
        conf = {
            "bucketizer_conf": [["custom_discretization", "-inf,-1,1,inf", "features", "features_bucketed", "False"]]}
        operator = BucketizerOperator(op_id="123", op_type="readtable", conf=conf, relation="", result_type="")
        dataset_list = operator.handle([dataset], self.spark)
        print("------------自定义离散  result:--------")
        print(conf)
        dataset_list[0].show()


if __name__ == "__main__":
    test = TestOperatorsAdditional()

    test.test_oneHotEncoderEstimatorOperator()
    print('\n')

    test.test_oneHotEncoderEstimatorOperator2()
    print('\n')

    test.test_splitOperator()
    print('\n')

    test.test_labelFeatureToLibsvm()
    print('\n')

    test.test_sampleOperator()
    print('\n')

    test.test_bucketizerOperator()
    print('\n')