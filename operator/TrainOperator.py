from abc import abstractmethod
from Operator import Operator


class TrainOperator(Operator):

    def __init__(self, op_id, op_type, conf, relation, result_type):
        self.op_category = "Train"
        Operator.__init__(self,
                          op_id,
                          op_type,
                          self.op_category,
                          conf,
                          relation,
                          result_type)

    @abstractmethod
    def handle(self, df_name_list):
        return

    def get_pre_df_list(self):
        params = []
        if len(self.pre_operator_list) > 0:
            for pre_operator in self.pre_operator_list:
                if pre_operator.op_type == "SeekHdfsOperator":
                    config = pre_operator.conf
                    output = config["output"]
                    params.append(output)
        else:
            print "error"

        return params

