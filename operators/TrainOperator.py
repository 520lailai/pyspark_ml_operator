# -*- coding: UTF-8 -*-
import collections
from abc import abstractmethod
from Operator import Operator


class TrainOperator(Operator):

    def __init__(self, op_id, op_type, conf, relation, result_type, user_defined=False):
        self.op_category = "Train"
        Operator.__init__(self,
                          op_id,
                          op_type,
                          self.op_category,
                          conf,
                          relation,
                          result_type,
                          user_defined)

    @abstractmethod
    def handle(self, df_name_list):
        return

    def get_pre_df_list(self):
        params = collections.OrderedDict()
        if len(self.pre_operator_list) > 0:
            for pre_operator in self.pre_operator_list:
                if pre_operator.op_type == "SaveHdfsOperator":
                    config = pre_operator.conf
                    pre_op_id = pre_operator.op_id

                    output = config["output"]
                    if self.op_type == "XgboostOperator":
                        relation = self.relation
                        for stitch in relation:
                            if "1" == str(stitch):
                                data_type = "train"
                            else:
                                data_type = "test"
                            if relation[stitch]["op_id"] == pre_op_id:
                                params[data_type] = output
                    else:
                        params["data"] = output
                elif pre_operator.op_type == "XgboostOperator":
                    config = pre_operator.conf
                    if "output" in config:
                        model = config["output"]
                        params["model"] = model
                    else:
                        raise Exception("param error")
                else:
                    raise Exception("%s preposition operator type error: required SaveHdfsOperator type ,"
                                    " given %s type" % (pre_operator, pre_operator.op_type))
        else:
            raise Exception("Operator %s get arguments error. " % self.op_id)
        print params
        return params

