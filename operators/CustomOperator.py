from abc import abstractmethod
from DataProcessingOperator import DataProcessingOperator


class CustomOperator(DataProcessingOperator):

    def __init__(self, op_id, op_type, conf, relation, result_type, user_defined=True):
        DataProcessingOperator.__init__(self,
                                        op_id,
                                        op_type,
                                        conf,
                                        relation,
                                        result_type,
                                        user_defined)

    @abstractmethod
    def handle(self, df_name_list, spark_session):
        return
