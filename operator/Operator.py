from abc import abstractmethod


class Operator:

    def __init__(self,
                 op_id,
                 op_type,
                 op_category,
                 conf,
                 relation,
                 result_type="single"):

        self.op_id = op_id
        self.op_type = op_type
        self.op_category = op_category
        self.status = "inited"
        self.result_type = result_type
        self.pre_operator_list = []
        self.next_operator_list = []
        self.result_df_name_list = []
        self.conf = conf
        self.relation = relation

    @abstractmethod
    def handle(self, df_name_list, spark_session):
        return

    @abstractmethod
    def get_pre_df_list(self):
        return

