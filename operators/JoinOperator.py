# -*- coding: utf-8 -*-
from DataProcessingOperator import DataProcessingOperator
from OperatorsUtils import *

''' 
    conf[]：
        join_columns :  list[String]   ex: [["id1", "id2"],["country","country"]]
        select_left_columns :  list[String]   ex: [["id1","id1"],["hour1", "hour1"],["country", "country1"]]
        select_right_columns : list[String]   ex: [["id2","id2"],["hour2", "hour2"],["country", "country2"]]
        join_type : String   ex: 'inner'
    dataframe_list:  [t1, t2]
'''


class JoinOperator(DataProcessingOperator):
    # 参数：withReplacement:boolean, fraction:String，seed:String
    def handle(self, dataframe_list, spark):
        join_columns = self.conf["join_columns"]
        select_left_columns = self.conf["select_left_columns"]
        select_right_columns = self.conf["select_right_columns"]
        join_type = self.conf["join_type"]

        df1 = dataframe_list[0]
        df2 = dataframe_list[1]
        check_dataframe(df1)
        check_dataframe(df2)

        if not join_columns:
            raise ParameterException("the parameter is empty:" + str(join_columns))

        select_colums_list = []

        # select left_columns
        left_columns_dict = {}
        for col in select_left_columns:
            left_columns_dict[col[0]] = col[1]
            df1 = df1.withColumnRenamed(col[0], col[1])
            select_colums_list.append(df1[col[1]])

        # select right_columns
        right_columns_dict = {}
        for col in select_right_columns:
            right_columns_dict[col[0]] = col[1]
            df2 = df2.withColumnRenamed(col[0], col[1])
            select_colums_list.append(df2[col[1]])

        # express_list
        express_list = []
        for two_colums in join_columns:
            express_list.append(df1[left_columns_dict[two_colums[0]]] == df2[right_columns_dict[two_colums[1]]])

        dataframe = df1.join(df2, express_list, join_type).select(select_colums_list)
        return [dataframe]
