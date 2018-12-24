import collections
from abc import abstractmethod
from Operator import Operator


class DataProcessingOperator(Operator):

    def __init__(self, op_id, op_type, conf, relation, result_type, user_defined=False):
        self.op_category = "DataProcessing"
        Operator.__init__(self,
                          op_id,
                          op_type,
                          self.op_category,
                          conf,
                          relation,
                          result_type,
                          user_defined)

    @abstractmethod
    def handle(self, df_name_list, spark_session):
        return

    def get_pre_df_list(self):

        params = collections.OrderedDict()

        if len(self.pre_operator_list) > 0:
            for pre_operator in self.pre_operator_list:
                if pre_operator.result_type == "single":
                    if len(pre_operator.result_df_name_list) > 0:
                        params.setdefault(pre_operator.op_id, []).append(0)
                    else:
                        print "The prefix operator has no output or no real preposition operator"
                else:
                    relations = pre_operator.relation
                    if relations is not None:
                        if len(relations) > 0:
                            for stitch in relations:
                                op_items = relations[stitch].split(",")
                                if len(op_items) > 0:
                                    for item in op_items:
                                        if item == self.op_id:
                                            params.setdefault(pre_operator.op_id, []).append(int(stitch)-1)
                                else:
                                    raise Exception("%s preposition operator result_type is multi, but no relation conf.") % self.op_id
                                    #params[pre_operator.op_id] = int(relations[self.op_id])-1
        return params

