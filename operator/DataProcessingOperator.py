import collections
from abc import abstractmethod
from Operator import Operator


class DataProcessingOperator(Operator):

    def __init__(self, op_id, op_type, conf, relation, result_type):
        self.op_category = "DataProcessing"
        Operator.__init__(self,
                          op_id,
                          op_type,
                          self.op_category,
                          conf,
                          relation,
                          result_type)

    @abstractmethod
    def handle(self, df_name_list, spark_session):
        return

    def get_pre_df_list(self):

        params = collections.OrderedDict()

        if len(self.pre_operator_list) > 0:
            for pre_operator in self.pre_operator_list:
                if pre_operator.result_type == "single":
                    if len(pre_operator.result_df_name_list) > 0:
                        params[pre_operator.op_id] = 0
                    else:
                        print "The prefix operator has no output or no real preposition operator"
                else:
                    relations = pre_operator.relation
                    if relations is not None:
                        params[pre_operator.op_id] = int(relations[self.op_id])-1
        return params

