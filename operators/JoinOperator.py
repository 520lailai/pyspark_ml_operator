# -*- coding: utf-8 -*-
from Operator import Operator
import logging
from OperatorsUtils import *

''' 
    conf[]：
        join_columns :  list[String]   ex: "1.id1==2.id2,1.country==2.country"
        select_column : list[String]   ex: "1.id1,1.country,1.hour1,1.clicked1,2.hour2,2.clicked2"
        join_type : String             ex: 'inner'
    dataframe_list:  [t1, t2]
'''


class JoinOperator(Operator):
    # 参数：withReplacement:boolean, fraction:String，seed:String
    def handle(self, dataframe_list, spark):
        join_columns = self.conf["join_columns"]
        select_columns = self.conf["select_columns"]
        join_type = self.conf["join_type"]

        df1 = dataframe_list[0]
        df2 = dataframe_list[1]
        check_dataframe(df1)
        check_dataframe(df2)

        check_str_parameter(join_columns)
        check_str_parameter(select_columns)
        check_str_parameter(join_type)

        try:
            # express_list
            join_columns_list = join_columns.replace("1.", "").replace("2.", "").split(",")
            express_list = []
            for kv in join_columns_list:
                kv_array = kv.split("==")
                express_list.append(df1[kv_array[0]] == df2[kv_array[1]])
            # select col_list
            cols = select_columns.split(",")
            col_list = []
            for col in cols:
                if "1." in col:
                    col_list.append(df1[col.replace("1.", "")])
                elif "2." in col:
                    col_list.append(df2[col.replace("2.", "")])
        except Exception as e:
            logging.exception(e)
            raise ParameterException("intput parameter error, pelease check again", e)

        dataframe = df1.join(df2, express_list, join_type).select(col_list)
        return [dataframe]
