# -*- coding: utf-8 -*-
from Operator import Operator
from OperatorsUtils import *

''' conf[]：
       column_names:      List[String],               ex:  "id1,hour1,clicked1"
       filter_condition : String, where expression    ex: "hour1>=10 and country == 'US'"
    dataframe_list:  []
'''


class SelectOperator(Operator):

    def handle(self, dataframe_list, spark):
        column_names = self.conf["column_names"]
        filter_condition = self.conf["filter_condition"]
        df = dataframe_list[0]

        check_dataframe(df)
        check_strlist_parameter(column_names, "the parameter:column_name is null!")
        check_str_parameter(filter_condition, "the parameter:filter_condition is null!")

        column_names = str_convert_strlist(column_names)


        dataframe = df.select(column_names).filter(filter_condition)
        return [dataframe]
